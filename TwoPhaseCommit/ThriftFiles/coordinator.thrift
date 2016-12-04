
exception SystemException {
  1: optional string message
}

enum Status {
  FAILED = 0;
  SUCCESSFUL = 1;
}

struct StatusReport {
  1: required Status status;
  2: optional string message;
}

struct RFile {
  1: required i32 tid;
  2: required string filename;
  3: required string content;
}

service Coordinator {

  StatusReport writeFile(1: RFile rFile)
    throws (1: SystemException systemException),

  RFile readFile(1: string filename)
    throws (1: SystemException systemException),

  string getFinalVotingDecision(1: i32 tid)
	throws (1: SystemException systemException),
}
