"""Remove redundant indexes from the headless workflow tables.

Revision ID: 0012_consolidate_workflow_indexes
Revises: 0011_mutation_approvals
"""

from alembic import op


revision = "0012_consolidate_workflow_indexes"
down_revision = "0011_mutation_approvals"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("ix_interview_questions_role", table_name="interview_questions")
    op.drop_index(
        "ix_interview_sessions_application_id", table_name="interview_sessions"
    )
    op.drop_index(
        "ix_company_candidates_discovered_at", table_name="company_candidates"
    )


def downgrade() -> None:
    op.create_index(
        "ix_company_candidates_discovered_at", "company_candidates", ["discovered_at"]
    )
    op.create_index(
        "ix_interview_sessions_application_id", "interview_sessions", ["application_id"]
    )
    op.create_index(
        "ix_interview_questions_role", "interview_questions", ["role_focus"]
    )
