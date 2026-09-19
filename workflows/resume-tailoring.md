# Resume and cover-letter tailoring

Use approved profile facts and captured job evidence as the only source for a
document proposal. Call `create_document_draft`, show its provenance and
content hash, and treat the result as pending until the user explicitly
approves that exact version with `approve_document`.

Never invent accomplishments, credentials, dates, or requirements. A changed
fact set or job description invalidates the proposal and requires a fresh
draft; Jobby does not send documents or submit applications.
