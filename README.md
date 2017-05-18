# lostifier
Setup tools and utilities for Spatial Router ECRF and LVF

### Get the code.
Clone the repo locally

`git clone TBD`


### First time setup of your python environment.  
`cd lostifier`  
`virtualenv --python c:\Users\<your user>\AppData\Local\Programs\Python\Python36\python.exe venv`  
`.\venv\Scripts\activate`  
`pip install -r requirements.txt`

After you've done this once, you just need to activate the environment when you work on it later.

`.\venv\Scripts\activate'

If you want to exit out of the current python envoronment, use `deactivate`.

`deactivate`

If/when you add new python packages to the python environment, make sure to regenerate the requirements.txt file.

`pip freeze > requirements.txt`

