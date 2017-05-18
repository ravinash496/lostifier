# lostifier
Setup tools and utilities for Spatial Router ECRF and LVF

### General code workflow - first time working on a project.
1. Clone the repo locally

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

