# from flask import Flask, render_template
from flask import Flask

app = Flask(__name__)

@app.route('/')
def hello():
	return 'Hello, World! (from app.py)'

if __name__ == '__main__':
  app.run()
