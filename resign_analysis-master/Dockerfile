FROM python:3.7
  
COPY ./requirements.txt .
RUN pip install --user -r requirements.txt
  
RUN mkdir -p /tmp/Modeling
WORKDIR /tmp/Modeling
   
# 更改Docker Container的時區成台北的時區
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends tzdata
  
RUN TZ=Asia/Taipei \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo $TZ > /etc/timezone \
    && dpkg-reconfigure -f noninteractive tzdata
  
# Docker Container 啟動時所執行的程序
CMD ["/bin/bash"]
