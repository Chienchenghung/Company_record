# Build Stage
# Docker Image來源FROM python:3.6-slim
FROM python:3.8-slim as builder
  
# 安裝所需要的Python套件
COPY ./requirements.txt .
RUN pip install --user -r requirements.txt
  
# Production Stage
# Docker Image來源FROM python:3.6-slim
FROM python:3.8-slim as app
   
# 創立docker container volume位置的資料夾，方便管理
RUN mkdir -p /tmp/reward_list
WORKDIR /tmp/reward_list
   
# 將所需要的套件移轉至Production Stage
COPY --from=builder /root/.local /root/.local
ENV PATH=/root/.local/bin:$PATH
  
# 更改Docker Container的時區成台北的時區
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends tzdata
  
RUN TZ=Asia/Taipei \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo $TZ > /etc/timezone \
    && dpkg-reconfigure -f noninteractive tzdata
	
RUN apt-get update && apt-get install libgomp1
  
# Docker Container 啟動時所執行的程序
CMD ["/bin/bash"]