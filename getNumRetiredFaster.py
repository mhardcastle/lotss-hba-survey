#!/usr/bin/python

from __future__ import print_function
import requests
import panoptes_client
from panoptes_client import Panoptes, Subject, SubjectSet, Project, Workflow, SubjectWorkflowStatus
from panoptes_client.set_member_subject import SetMemberSubject
import getpass
import glob
import numpy as np
import os
from surveys_db import SurveysDB
from time import sleep

user = 'mjh22'
password = os.environ['PANOPTES_PASSWORD']
client = Panoptes.connect(username=user, password=password)
projectId = 8190
assocAndIdWorkflow = 11973

while True:
    try:
        project = Project.find(id=projectId)
        print(project.display_name)
        workflow=Workflow(assocAndIdWorkflow)
        ssets = workflow.links.subject_sets # not project!
        workflows = project.links.workflows

        workflowStatuses = SubjectWorkflowStatus.where(
            workflow_id=assocAndIdWorkflow, timeout=1000
        )
        subjectRetirementStats = {
            workflowStatus.raw["links"]["subject"]: workflowStatus.raw["retired_at"]
            for workflowStatus in workflowStatuses
        }

        idsForSSets = {
            sset: [
                subjectSetMember.raw["links"]["subject"]
                for subjectSetMember in SetMemberSubject.where(subject_set_id=sset.id)
            ]
            for sset in ssets
        }

        ssetRetirementStats = {
            sset.display_name: np.sum(
                [
                    subjectRetirementStats[subject] is not None
                    for subject in subjects
                    if subject in subjectRetirementStats
                ]
            )
            for sset, subjects in idsForSSets.items()
        }
    except panoptes_client.panoptes.PanoptesAPIException as e:
        print('Panoptes API exception:',e)
        sleep(60)
        continue
    except requests.exceptions.ConnectionError as e:
        print('Connection error:',e)
        sleep(60)
        continue
        
    #print(ssetRetirementStats)

    total=0
    with SurveysDB() as sdb:
        for k in ssetRetirementStats:
            if k[0]=='P':
                r=sdb.get_field(k)
                print(k,ssetRetirementStats[k],r['rgz_sources'],r['weave_priority'])
                total+=1
                if r is not None:
                    r['rgz_complete']=ssetRetirementStats[k]
                    if r['rgz_complete']==r['rgz_sources']:
                        r['gz_status']='Complete'
                    sdb.set_field(r)
                    

    print('Total fields',total)
    sleep(3600)
    
        
