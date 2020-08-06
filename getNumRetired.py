from __future__ import print_function
from panoptes_client import Panoptes, Subject, SubjectSet, Project, SubjectWorkflowStatus
import os

user = 'mjh22'
password = os.environ['PANOPTES_PASSWORD']
client = Panoptes.connect(username=user, password=password)

projectId = 8190
project = Project.find(id=projectId)
# Check that we're not making a mistake!
print(project.display_name)

ssets = project.links.subject_sets
workflows = project.links.workflows

# Check that we're not making a mistake!
for workflow in workflows:
    if workflow.display_name == "Associate and identify":
        assocAndIdWorkflow = workflow.id
        print(workflow.display_name, workflow.id, "*")
    else:
        print(workflow.display_name, workflow.id)

retirementCounts = {}
for sset in ssets:
    # Guessing a sensible filter for subject set names.
    if sset.display_name.startswith("P"):
        retirementCounts[sset.display_name] = 0
        for subject in sset.subjects:
            retirementCounts[sset.display_name] += (next(
            SubjectWorkflowStatus.where(subject_id=subject.id, workflow_id=assocAndIdWorkflow)).retired_at is not None)
        print(sset.display_name, retirementCounts[sset.display_name])

print(retirementCounts)
