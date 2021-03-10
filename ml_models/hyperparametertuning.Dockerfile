FROM nvidia/cuda:11.2.1-base
COPY cache/recent_change_rtd_hyper_dataset /app/cache/recent_change_rtd_hyper_dataset

#set up environment
RUN apt-get update -y && apt-get install -y gcc
RUN apt-get install -y python3.8
RUN apt-get install -y python3-pip
# RUN apt-get install nvidia-container-runtime
# RUN pip3 install cython
# RUN pip3 install setuptools
# RUN pip3 install sklearn

COPY ml_models/hyperparametertuning_requirements.txt /app/hyperparametertuning_requirements.txt
RUN pip3 install -r /app/hyperparametertuning_requirements.txt
#copies the applicaiton from local path to container path
COPY cache/recent_change_rtd_ar_cs_encoder_dict.pkl /app/cache/recent_change_rtd_ar_cs_encoder_dict.pkl
COPY cache/recent_change_rtd_dp_cs_encoder_dict.pkl /app/cache/recent_change_rtd_dp_cs_encoder_dict.pkl
COPY ml_models /app/ml_models
COPY helpers /app/helpers
COPY database /app/database
WORKDIR /app
CMD ["python3", "ml_models/hyperparametertuning_xgboost.py"]