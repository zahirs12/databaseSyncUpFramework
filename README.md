# databaseSyncUpFramework
This framework design to sync up Oracle Database schema with DB objects like Tables, Indexes, Store Procedure, Functions and Privilege.
Developed innovative solution for ABC project, which saves approximately $26K annually.

Problem Statement # ABC Migration Environment Sync Up activities take time.
  1.	Ideal Situation # Ideally Team should be able to sync up all four environment in two hours.
  2.	Reality # In reality team spends approximately 32 hours to sync the four ABC Migration Project Environment. Initially team used to       work on two products when project started and had limited number of objects but gradually more products have been added thus the         environment for each product has defined many new objects increased.
  3.	Consequences #
      I.	Cost Spent -- Team waste approximately 30 hours for each delivery and every month almost we have two delivery. 
          The company’s standard saving template as below
          Billable Cost of Resource 1 Hr = $36.00 ; Time Saved = 30 Hrs AND Annual = 24 Project Delivery
          $ Total Annual Saving     = (Billable Cost * Time Saved * Delivery_days_in_a_year)
                                    = 36 * 30 * 24
                                    = $25,920.00 
      II.	Network Congestions – Offshore team sync up environment by connecting client application with server i.e. SQLDeveloper, WinSCP           etc.. so first objects brought to local machine do the comparisons and then push to server for the modified objects. This               activity uses the network and it hamper the performance. 
      III.	Human-error Mistake – Since developer is manually setup the environment so there might be chance of human negligence. 
            Proposal #  Need to build the Automated Solution, which compare the source objects with destination objects and pushes the               missing/modified objects to target environment.
               
Solution # 
   1.	Identification# What all the objects are needed for environment setup, backup of an existing code and purging of old backup             folders.
   2.	Challenges # 
      I.	As database is on VM Server and team doesn’t have access to storage of it. Thus can’t use PL/SQL block in order to write the             Audit file of DB object’s comparisons.
      II.	Need to find out the Oracle Database’s metadata dictionary, in order to extract, compare and push the DDL of objects – Tables,           Indexes, Store Procedure, Functions and Privileges etc.. to destination from source schema.
      III.	Having difficulties in transferring excel files as its name having space or special characters.
   3.	Development # Developed generic and highly optimize shell & python script to take care of Sync Up activities and it’s running on         server.
   4.	Performance #  This framework takes less than 5 mins for all the environment to sync up by executing concurrently.

Conclusion # Team focus on project requirement and Environment Sync up tasks take care by this framework.

