import pickle
import os
import argparse

def main( dicomodel ):
    with open(dicomodel,'rb+') as f:
        d = pickle.load(f,encoding='latin1')
        weight_dict = d['GD']["Weight"]
        if not 'EnableSigmoidTaper' in weight_dict.keys():
            print('keys are missing from the Weight dict, adding')
            d['GD']["Weight"]["EnableSigmoidTaper"]=0
            d['GD']["Weight"]["SigmoidTaperInnerCutoff"]=0
            d['GD']["Weight"]["SigmoidTaperOuterCutoff"]=0
            d['GD']["Weight"]["SigmoidTaperInnerRolloffStrength"]=0.5
            d['GD']["Weight"]["SigmoidTaperOuterRolloffStrength"]=0.5
            with open('new.dicomodel','wb') as f:
                pickle.dump(d,f)
            ## move the file to the original name
            os.system('mv new.dicomodel {:s}'.format(dicomodel))
        else:
            print('dicomodel has all required keywords, nothing to be done.')


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('dicomodel',type=str)
    args = parser.parse_args()
    main(args.dicomodel)
