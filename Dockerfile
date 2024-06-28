FROM python:3.11-slim

WORKDIR /usr/src/app

COPY . .

RUN chmod +x start.sh

RUN pip install -r requirements.txt

EXPOSE 7860

CMD ["./start.sh"]


