# sorting-hat
Demo code for using Heroku to integrate Salesforce and third party data.


This is sample code from a Dreamforce 16 presentation on integrating Salesforce with 3rd party and large data sets on Heroku.

It's not ready to run out of the box, but should give you an idea of how to get started.

In order to get this running, you would need to set up a Salesforce developer org, have a Heroku account and create an app in Heroku for this
codebase. You'll need a Postgres database with at least one table configured to match your Salesforce object(s) that you'd like to synch.  

Check out the SFListener class to see how to configure synching for the object(s) and field(s) you need.  Happy Coding!
