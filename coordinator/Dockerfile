FROM python:3.12-slim
WORKDIR /app
# No dependencies: XML-RPC is part of Python's standard library.
COPY node.py .
EXPOSE 9000
CMD ["python", "-u", "node.py"]
