Promts used for claude.
## Session 1 

*(Shared the project instructions slide)* Can you suggest some project ideas for a data streaming system that meet these requirements?
I think I'll go with idea 3, the Port of Hamburg ship-traffic anomaly detection.
We'll start the setup and coding tomorrow.
I've installed Docker. What's the next step?
What happens if the live data stream fails?
What does Kafka actually do in this pipeline?

## Session 2 

 *(Shared the feedback)* My professor wants the speeding rule and the status-mismatch rule removed, the signal-gap threshold reduced to 1 minute, and a new rule based on counts, cumulative numbers, mean, median, histograms and a dynamic threshold. He also wants us to explain what happens when the consumer goes down and how it recovers, and how duplicate data from the producer is handled (idempotency).
 So what are the three rules now? He wants something useful from a business point of view, something that grows cumulatively and shows mean, median and so on.
 That sounds good, let's go with these rules.
 What exactly is aisstream.io used for?
 `pip install -r requirements.txt` says the file doesn't exist. What am I doing wrong?
 Do I keep the same docker-compose file, or does it change because of my professor's comments?
`docker compose up -d` fails with "failed to connect to the docker API". What does that mean?
Kafka started, but creating the topics fails with "container is not running". Why?
These are the Kafka logs: "advertised.listeners cannot use the nonroutable meta-address 0.0.0.0". What's wrong?
`docker ps` now shows kafka and kafka-ui running. What's next?
The producer keeps printing "Stream lost… Reconnecting". Why does the connection keep dropping?
My aisstream.io account page says "No API keys yet". Is that the problem?
Should I create an API key?
The producer is running now and shows sent, duplicates and throughput. Is this correct?
How do I run these scripts?
The detector prints a lot of "going dark" alerts with silences of more than 1000 seconds. Is that right?
I'm back, let's finish the project.
PowerShell doesn't recognise `detector.py`. How do I run it?
Here is the output after resetting the topics and consumer offsets (step 3). Did it work?
`python stats.py` gives a SyntaxError: "unterminated triple-quoted string literal". How do I fix it?
`streamlit run dashboard.py` says the file doesn't exist. What's wrong?
The dashboard is running and shows the live map. Does everything look right?
I replaced the dashboard file. Here is the zoomed map.


## Session 3 Testing, measurements, documentation

The detector shows a warning "Coordinator load in progress" when it starts. Is this an error?
The detector is running and shows processed, ships tracked and latency. Is this working correctly?
The consumer group shows one consumer owning all three partitions. Is that expected?
What are the steps we need to complete before we have everything for the README?
Can you send me the latest version of detector.py?
Here is the producer output with the throughput numbers.
Why do we run the detector in two terminals?
Here is the output of the first and the second detector.
What does the `kafka-consumer-groups --describe` command do?
Here is the describe table with two consumers sharing the partitions.
Where do I run the describe command?
I stopped both detectors. The describe output says "no active members" and shows lag 46 / 43 / 50.
Do you want me to run the describe command again?
After restarting one detector, the lag is 0 on all partitions.
Here is the stats.py output after it caught up, including some long-wait alerts.
I'm doing the stress test now.
What does this stress test actually show?
The replay at 20× speed reached about 22 messages per second for 6,298 messages, and the describe table shows lag 0.