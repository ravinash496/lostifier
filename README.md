# lostifier
Bulkload, setup tools and utilities for Spatial Router.

### General code workflow - first time working on a project.
1. Clone the repo locally.

`git clone https://github.com/Geo-Comm/lostifier.git`  

2. Make a branch to contain your changes and create the remote branch to push to.

`git checkout -b <branch name>`  
`git push --set-upstream origin <branch name>`  

3. Go to work and make your changes.

4. When finished with some unit of work, add your changes to the current change set and commit them.

`git add .`  
`git commit -m "some commit message"`  

This will commit them to your local branch.

5. Push your changes to the remote branch.

`git push`  

6. Issue a pull request to have the changes merged into the master.

7. If you are merging the pull request, make sure to tell github to delete the branch once the merge is complete.

8. Once the merge to master is done, you can delete your local branch.

`git checkout master`  
`git branch -d <branch name>`  

### General code workflow - working on a project you've worked on before.

1. Get the latest changes from the remote origin (master).

`git pull`  

2. If you have not done so already, create local and remote branches for your work.

`git checkout -b <branch name>`  
`git push --set-upstream origin <branch name>`  

3. Go to work and make your changes.

4. When finished with some unit of work, add your changes to the current change set and commit them.

`git add .`  
`git commit -m "some commit message"`  

This will commit them to your local branch.

5. Push your changes to the remote branch.

`git push`  

6. Issue a pull request to have the changes merged into the master.

7. If you are merging the pull request, make sure to tell github to delete the branch once the merge is complete.

8. Once the merge to master is done, you can delete your local branch.

`git checkout master`  
`git branch -d <branch name>`  


### First time setup of your python environment - Ubuntu Linux.  
`$ cd lostifier`  
`$ virtualenv --python python3.6 venv`  
`$ source ./venv/bin/activate`  

For linux, you need to get gdal and install the python stuff on top of it.
(Credit to http://www.sarasafavi.com/installing-gdalogr-on-ubuntu.html for some of the steps here.)

`$ sudo add-apt-repository ppa:ubuntugis/ppa && sudo apt-get update`  
`$ sudo apt-get install gdal-bin`  
`$ sudo apt-get install libgdal-dev`  
`$ export CPLUS_INCLUDE_PATH=/usr/include/gdal`  
`$ export C_INCLUDE_PATH=/usr/include/gdal`  
`$ pip install gdal==2.1.0`  
`$ pip install psycopg2`  

Now install the rest of the dependencies.
`pip install -r requirements.txt`  

### First time setup of your python environment - Windows.  
Download the python wheel file for GDAL
    a.Go to http://www.lfd.uci.edu/~gohlke/pythonlibs/#gdal
    b.Locate the link for GDAL-2.1.3-cp36-cp36m-win_amd64.whl and click on it.
    c.Note the location of the download for later.
	
Download the python wheel file for Psycopg
    a.Go to http://www.lfd.uci.edu/~gohlke/pythonlibs/#Psycopg
    b.Locate the link for psycopg2-2.7.1-cp36-cp36m-win_amd64.whl and click on it.
    c.Note the location of the download for later.

Assuming you've pulled the repo from github, opend up a powershell terminal as administrator.

Create and activate the python environment.
`cd lostifier`  
`virtualenv --python c:\Users\<your user>\AppData\Local\Programs\Python\Python36\python.exe venv`  
`.\venv\Scripts\activate` 

Install the GDAL and Psycopg packages downloaded above.
`pip install c:\Users\<your user>\Downloads\GDAL-2.1.3-cp36-cp36m-win_amd64.whl`  
`pip install c:\Users\<your user>\Downloads\psycopg2-2.7.1-cp36-cp36m-win_amd64.whl`  

Nos install the rest of the dependencies.
`pip install -r requirements.txt`  

### Other stuff . . .

If/when you add new python packages to the python environment, make sure to regenerate the requirements.txt file.

`pip freeze > requirements.txt`  

Whenever you come back to work on the project, activate the python environment.

Linux:
`$ source ./venv/bin/activate`  

Windows:
`.\venv\Scripts\activate`  

If you want to exit out of the current python envoronment, use 

`deactivate`  


