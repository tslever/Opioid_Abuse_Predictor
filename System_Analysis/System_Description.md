# System Description

*Opioid-Abuse Predictor*

Created: 06/23/2023 by Tom Lever

Updated: 06/01/2025 by Tom Lever 

## Context: Previous Research

The opioid epidemic refers to the rampant misuse and addiction to opioid drugs, leading to devastating consequences on public health. Doctors may be uncomfortable prescribing opioids due to concerns about potential misuse, addiction, and the risk of overdose associated with these medications. Opioids, such as oxycodone, hydrocodone, morphine, and methadone, are prescribed by medical providers to alleviate severe symptoms including constipation, respiratory distress, and more commonly pain. Although effective, the adverse effects of opioids, such as dependence, abuse, and poisoning, have resulted in 130 deaths of Americans daily and impose significant financial and resource burdens on the healthcare system. Since approximately 91.8 million (37.8%) civilian non-institutionalized adults in the United States consumed prescription opioids in 2015 and 13.5 million (5.5%) of them had some form of opioid misuse, determining individuals who are at an increased susceptibility to these conditions is essential for responsible pain and system management (Han et al. 2015). To support providers in clinical decision making, statistical methods and machine learning techniques can analyze vast amounts of electronic health record (EHR) data and identify patterns and risk factors of aberrant drug-related behavior (ADRB) associated with opioids. These models can help predict individuals at a higher risk of dependence, abuse, and poisoning, enabling providers to tailor their prescribing practices and interventions accordingly. By utilizing these tools, medical providers can strike a balance between effectively managing pain and symptoms while prioritizing patient safety and mitigating the negative consequences of opioid use.

## Opportunity Opioid-Abuse Predictor Will Address

In this study, we aim to improve upon the general objective of the Opioid Risk Tool (ORT), which is to predict prior to a patient receiving an opioid prescription the probability of the patient exhibiting opioid-related ADRB (opioid abuse). There is a relative scarcity of machine learning studies that focus on the same patient population; i.e, individuals being evaluated prior to starting opioid medication. Addressing this gap is crucial as identifying risk factors prior to prescription can significantly aid in preventing the need for opioid tapering. Opioid tapering, while a logical intervention, can paradoxically worsen pain, function, and psychiatric symptoms due to protracted abstinence syndrome. Therefore, focusing on predicting opioid abuse is crucial, considering its potential to initiate dependence and the complexity of opioid dependence diagnoses. Furthermore, this project will expand our focus to include all individuals without cancer who have been prescribed opioids at least once in their medical history who may be prescribed opioids. Accordingly, we will predict the probability of opioid abuse in individuals with severe constipation, respiratory distress, and chronic non-malignant pain (CNMP), unlike the ORT, which only examines individuals with CNMP.

Our Opioid-Abuse Predictor aims to mobilize NIH researcher workbench EHR data to serve as a modern and user-friendly foundation for future research endeavors. Our Opioid-Abuse Predictor aims to apply advanced technologies such as LSTM, RNNs, and factor analysis to explore the critical temporal dependencies among medication, procedure, lab results, and diagnosis histories, leading to more precise predictions of opioid abuse probabilities.

Ultimately, the Opioid-Abuse Predictor aims to function as a vital tool for healthcare providers, offering clinical decision-making support. Its purpose is to enhance patient safety by providing more informed guidance to providers when prescribing opioids. 

## What Opioid-Abuse Predictor Will Do

The Opioid-Abuse Predictor will be a system that a Requester may use prior to any prescription of opioids to submit a Patient and receive a prediction of the probability that a patient will exhibit an opioid-related ADRB.

## Iterations Of Development
At the end of Iteration…
1. Opioid-Abuse Predictor will be as described in “What the Opioid-Abuse Predictor Will Do”.
