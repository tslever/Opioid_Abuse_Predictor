# To-Do’s for Developing Opioid-Abuse Predictor

Created: 06/23/2023 by Tom Lever

Updated: 07/23/2023 by Tom Lever


## Tasks For Tom That Morgan And Srimann Have Completed

1.	Read foundational literature on predicting opioid abuse using techniques like factor analysis and models like RNN's.
2.	~~Gain access to NIH Researcher Workbench.~~
3.	~~Experiment with NIH Researcher Workbench.~~
4.	~~Watch tutorials from Julia and Julia and NIH Researcher Workbench.~~
5.	~~Access data.~~
6.	~~Conduct basic EDA.~~


## Tasks For All (A) Of Morgan (M), Srimann (S), and Tom (T)

*Italics*: Requires Coordination
Non-Italics: Does Not Require Coordination

7.  *A: Receive Srimann and Morgan's presentations from Tuesday, 06/20/2023 on distinction between opioid abuse and dependence and general functions and code.*
8.  *A: Hear from Claudia Scholz about physicians to whom we can talk.*
9.  ~~*A: Meet with Julia to discuss previous research and Morgan and Srimann's research ideas on Monday, 06/26/2023*~~
10. ~*A: Complete System Description with sections for Context, Opportunity Opioid-Abuse Predictor Will Address, What Opioid-Abuse Predictor Will Do, and Iterations Of Development.*~
11. ~~*A: Complete System Diagram.*~~
12. ~~*A: Complete Use Case Description: Request Prediction Of Whether Patient Will Abuse Opioids.*~~
13. ~~*A: Complete Interface Design: Requester And Opioid-Abuse Predictor.*~~
14. ~~*A: Define Cohort_Without_Cancer.*~~
15. *A: Define feature matrix.*
    - Resolve discrepancies between numbers of rows in feature submatrices.
    - Modify `generate_slice_of_feature_matrix.py` such that a feature matrix has 0 instead of values that not numbers.
    - Modify `generate_slice_of_feature_matrix.py` to set seed of pseudorandom number generator so that the 3,790 patients chosen "at random" are the same for every generation of feature matrix.
    - Modify `generate_slice_of_feature_matrix.py` to impute 1's for chronic conditions.
    - Modify `generate_slice_of_feature_matrix.py` to impute "tails" for 1's to compensate for sparseness of 1's.
    - Ensure that each patient in feature matrix has a visit with a prescription of opioids.
17. M/T: Understand principal-component analysis, factor analysis, and partial least-squares regression for modeling, dimension reduction, data preprocessing, and determining the importance of features.
18. M/T: Design, implement, and tune principal-component analyses, factor analyses, and/or partial least-squares regressions.
19. *A: Choose a set of predictor transformations, and methods of aggregating predictors (e.g., Principal-Component Analysis, Factor Analysis).*
20. S/T: Understand motivation and ideas behind types of predictive models (e.g., factor analysis, RNN’s), architectures, and hyperparameters.
21. S/T: Design, implement, and tune predictive models. A model may include predictor transformations and methods of aggregating predictors.
22. *A: Choose how to evaluate performance (e.g., a balance of recall and precision).*
23. *A: Evaluate performance of models.*
24. *A: Choose type of model, architecture, and hyperparameters for final model.*
25. *A: Evaluate performance of final model.*
26. *A: Provide documentation for final model.*
27. *A: Write a summarizing paper.*
28. *A: Provide a summarizing presentation.*
