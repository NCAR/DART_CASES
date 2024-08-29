# DART_CASES
DART CASE directories from CESM+DART experiments.

This presents ways to archive the experiment setups for publication purposes
and curate what happens during the continued cycling of a CESM experiment.
Some (most?) things are expected to stay static, but better to be safe than confused.

Creating a CESM case setup under git control is reasonably straightforward.
Putting an existing case there is a little more complicated.

The following strategies are based on making the 'main' ('master') branch
of the git repository have no case files in it, and each experiment/case 
will be a unique branch named the same as the CESM 'CASE' name.
The separation is made clearer by also putting each case in a subdirectory
also named the same as the CASE.
The most useful way to do this is to make a new git clone for each new case.
This allows multiple cases to be active, and prevents a running job
from one case making changes to files belonging to a different branch.
All of these branches will be pushed to the same github repository.
They should not be merged into main, in order to keep the size of the branches small,
which allows more to fit within github's limit on free repositories.

## Terminology

| Word | Meaning|
 :--------------- |:-----------------------------------------------------------------------------------------|
|CASE        | your CESM CASE name, which is the name of the assimilation and the git branch |
|DART\_CASES | your github repository for archiving assimilation setups |
|CASE_git    | the local clone of DART\_CASES |
|EXP         | the directory where you want the CASE_git clones to be created |
|CASEROOT    | the path in which CESM builds CASE and from which assimilation jobs are launched.  'EXP/CASE_git/CASE' below |
|DART        | your clone of DART |

## Create a New CASE Under git(hub)

### Preliminaries

0. If you're not an employee in NSF NCAR's Data Assimilation Research Section,
   or are, but want to keep your cases separate from other DAReS CASEs,
   fork https://github.com/NCAR/DART_CASES.git to your github.
1. Put a copy of list\_of\_files\_to\_commit.csh (found in the top directory 
   of branch "main") into your shell\_scripts directory.
   Edit it ( or rewrite in your favorite language) to specify which files 
   should be under git control.  
   See comments in that file about how to choose the files.
2. In DART/models/\<your\_model\>/shell\_scripts/\<your\_setup\_script\>
   1. Set the CASE name for this assimilation.
      If you're **re**using a DART\_CASES-controlled case name, 
      clean the existing git branch, which might mean just

      ` git rm -r $CASE ; git branch -d $CASE `

   2. Specify that this case will be built in EXP/CASE\_git/CASE.
   3. Add lines like this before create\_newcase 

      ``` 
      git clone git@github.com:<your_github>/DART_CASES.git $EXP/${CASE}_git `
      git branch -a | grep $CASE
      if ($status == 0) exit
      ```
   4. add lines near the end to put CASE under git control:
      + cd \$CASEROOT
      + git checkout -b \$CASE
      + \$DART/\<your\_model\>/shell\_scripts/list\_of\_files\_to\_commit.csh
      + git commit -m "First commit of files in case \$CASE "

      This will create a *branch* in your CASE_git clone which has the name \$CASE.

### Production

3. Run the setup script
   This will create a *directory* in your CASE_git clone which has the name \$CASE.
   Follow the instructions about checking the setup.
4. If needed, in the CASEROOT directory, edit input.nml, \*.xml, etc.
   to define your assimilation job correctly.
   Add, commit, and push these to a new (-u) experiment git(hub) branch.

   ` git push -u origin ${CASE} `

5. Submit the job
6. If git-controlled files need to be changed for future DA cycles;
   add, commit, and push them to the (correct) branch.

   ` git push origin ${CASE} `

### Put an Existing CASE Under git(hub) Control in its Own Directory

Example commands follow this list of steps.

1.  Change the CASE directory name (made by CESM) to ${CASE}\_orig. (Not strictly necessary.)
2.  Clone the DART\_CASES repository into ${CASE}\_git.
3.  In ${CASE}\_git make a new *branch* with the name ${CASE} and check it out.
4.  Copy everything from ${CASE}\_orig into a new subdirectory ./${CASE} 
5.  Compare this copy with ${CASE}\_orig
6.  Make ${CASE}\_orig read-only, so that you don't accidentally use it.
7.  Update the CASEROOT variable in CESM.
8.  `git add` the files that should be archived.  You can use a variant of list\_of\_files\_to\_commit.csh
9.  Commit them (with a useful message)
10.  Push the contents of the CASE branch to DART\_CASES. There is a special syntax to push the new branch to the GitHub repository when the branch does not exist yet in the "origin".
11.  Ongoing; add, commit, and push any changes to the CASE which should be archived.


#### Tcsh Example

```
[step 0]% set CASE = <your_casename>

[step 1]% mv ${CASE} ${CASE}_orig

[step 2]% git clone git@github.com:<your_github>/DART_CASES.git ${CASE}_git

[step 3]% cd ${CASE}_git
[step 3]% git checkout -b ${CASE}

[step 4]% rsync -av ../${CASE}_orig/ ${CASE}

[step 5]% diff  -r  ../${CASE}_orig  ${CASE} | less

[step 6]% chmod 644 ../${CASE}_orig

[step 7]% xmlchange --caseroot <full_path_new_CASEROOT>

[step 8]% git add <files_you_want_to_archive>

[step 9]% git commit -m "First commit of files which must be archived"

[step 10]% git push -u origin ${CASE}

```

## Wild West Method

   If the methods above aren't available to you, then hopefully you can use parts of them
   to set up your case under git control.
   There are too many degrees of freedom to describe how to do it 
   1 operating system command at a time.


