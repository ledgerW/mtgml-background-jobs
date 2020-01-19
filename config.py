import os

stage = os.environ['stage']
resources_stage = os.environ['resources_stage']

stage_configs = {
  'dev': {
    STRIPE_SECRET_KEY: os.environ['STRIPE_SECRET_KEY_TEST']
    },
  'prod': {
    STRIPE_SECRET_KEY: os.environ['STRIPE_SECRET_KEY_PROD']
    }
};

config = stage_configs[stage] if stage else stage_configs['dev']
