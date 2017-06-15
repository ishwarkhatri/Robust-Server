# Robust-Server

Basic idea of the project is to build a Server that handles multiple requests parallely from many clients but also ensures no loss of data.

For any unexpected reasons if the Server goes down then the still makes sure that whenever the Server is up, all the pending/incomplete requests that were received should be completed without loss of information.

The Server comprises of one co-ordinator and 3 participants. Co-ordinator is responsible for handling all incoming requests and delegating them to participants.

On the other hand, participants are responsible to work on task alloted by Co-ordinator.

All participants send their execution results back to Co-ordinator and when the Co-ordinator has received positive response from all participants, only then it will ask them commit finally.

If even a single participant fails to execute the said task, then Co-ordinator will ask to abort the task to all other participants.

This implementation is called as Two Phase Commit Protocol and the complete server is built on this protocol.
