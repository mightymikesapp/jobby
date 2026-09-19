# Application preparation

Capture and evaluate the role, then call `create_application` only for the
user-selected job. Draft documents from approved facts and the job evidence;
the result is always pending. Show the diff, provenance, and content hash.

Approval requires the current document hash. If it changed, refresh the
proposal and ask again. Approved documents are immutable and may be attached
to applications, but Jobby never submits forms, sends mail, or bypasses MFA or
CAPTCHAs.
