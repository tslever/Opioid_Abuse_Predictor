Send me your email address for Research All Of Us
Navigate to https://workbench.researchallofus.org/
Create workspace Opioid_Abuse_Predictor with data-access tier Controlled Tier
Create a Cloud Analysis Environment with 4 CPU's, 15 GB RAM, Standard VM Compute, and Reattachable Persistent Disk of 120 GB.
Navigate to Cloud Analysis Terminal
Change directory to with cd /home/jupyter
Make directory with mkdir .ssh.
Change directory with cd .ssh.
Copy the contents of the attached Key_To_Opioid_Abuse_Predictor to clipboard.
Create file Key_To_Opioid_Abuse_Predictor with nano Key_To_Opioid_Abuse_Predictor.
Paste the contents of Key_To_Opioid_Abuse_Predictor in nano using Right Click and Paste.
Write Key_To_Opioid_Predictor and close nano with Control + X, Y, and Enter.
Give read and write permissions with chmod 600 Key_To_Opioid_Predictor.
Copy the contents of the attached Key_To_Opioid_Abuse_Predictor.pub to clipboard.
Create file Key_To_Opioid_Abuse_Predictor.pub with nano Key_To_Opioid_Abuse_Predictor.pub.
Paste the contents of Key_To_Opioid_Abuse_Predictor.pub in nano using Right Click and Paste.
Write Key_To_Opioid_Predictor.pub and close nano with Control + X, Y, and Enter.
Give read and write permissions with chmod 600 Key_To_Opioid_Predictor.pub.
Start SSH agent with eval "$(ssh-agent -s)".
Add SSH private key to SSH agent with ssh-add Key_To_Opioid_Abuse_Predictor.
Change directory with cd /home/jupyter/workspaces.
Clone Opioid_Abuse_Predictor with git clone git@github.com:tslever/Opioid_Abuse_Predictor.gitand yes.
Copy contents with cp -rT Opioid_Abuse_Predictor opioidabusepredictor.
Change directory with cd opioidabusepredictor.
Fetch with git fetch.
Pull with git pull.
Consider that there are no changes with git status.
Navigate to https://workbench.researchallofus.org/workspaces/aou-rw-0986d4b4/opioidabusepredictor/notebooks .
Create a new JuPyteR notebook called Hello_World.
Wait for the kernel to load.
In the first code cell, write print("Hello World!").
Run the first code cell.
Save the JuPyteR notebook.
Return to the terminal.
Consider the addition of Hello_World.ipynb with git status.
Add changes to staging area with git add Hello_World.ipynb.
Configure email with git config --global user.email "User@Domain"
Configure name with git config --global user.name "Name"
Commit with git commit -m "Added Hello_World.ipynb".
 Push with git push.
Fetch with git fetch.
Pull with git pull.
Consider that there are no changes with git status.
Navigate to https://workbench.researchallofus.org/workspaces/aou-rw-0986d4b4/opioidabusepredictor/notebooks .
Delete Hello_World.ipynb.
Return to the terminal.
Remove with rm Hello_World.ipynb.
Consider the removal of Hello_World.ipynb with git status.
Add changes to staging area with git add Hello_World.ipynb.
Commit with git commit -m "Removed Hello_World.ipynb".
Push with git push.
Fetch with git fetch.
Pull with git pull.
Consider that there are no changes with git status.