"""Add headless interview, question-bank, and company discovery workflows.

Revision ID: 0010_headless_workflows
Revises: 0009_release_0_6
"""

from alembic import op
import sqlalchemy as sa


revision = "0010_headless_workflows"
down_revision = "0009_release_0_6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "interview_questions",
        sa.Column("prompt", sa.Text(), nullable=False),
        sa.Column("role_focus", sa.String(length=300), nullable=True),
        sa.Column("tags", sa.JSON(), nullable=False),
        sa.Column("skills", sa.JSON(), nullable=False),
        sa.Column("evidence_keys", sa.JSON(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_interview_questions_role", "interview_questions", ["role_focus"]
    )
    op.create_index(
        "ix_interview_questions_role_focus", "interview_questions", ["role_focus"]
    )
    op.create_table(
        "interview_sessions",
        sa.Column("application_id", sa.String(length=36), nullable=False),
        sa.Column("interview_id", sa.String(length=36), nullable=True),
        sa.Column("session_type", sa.String(length=80), nullable=False),
        sa.Column("role_focus", sa.String(length=300), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("retrospective", sa.Text(), nullable=True),
        sa.Column("outcome", sa.String(length=100), nullable=True),
        sa.Column("follow_up_task_id", sa.String(length=36), nullable=True),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["application_id"], ["applications.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["interview_id"], ["interviews.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["follow_up_task_id"], ["tasks.id"], ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_interview_sessions_application",
        "interview_sessions",
        ["application_id", "started_at"],
    )
    op.create_index(
        "ix_interview_sessions_application_id", "interview_sessions", ["application_id"]
    )
    op.create_index(
        "ix_interview_sessions_interview_id", "interview_sessions", ["interview_id"]
    )
    op.create_index(
        "ix_interview_sessions_follow_up_task_id",
        "interview_sessions",
        ["follow_up_task_id"],
    )
    op.create_table(
        "interview_answers",
        sa.Column("session_id", sa.String(length=36), nullable=False),
        sa.Column("question_id", sa.String(length=36), nullable=True),
        sa.Column("answer", sa.Text(), nullable=False),
        sa.Column("evidence_refs", sa.JSON(), nullable=False),
        sa.Column("tags", sa.JSON(), nullable=False),
        sa.Column("rating", sa.Integer(), nullable=True),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "session_id", "question_id", name="uq_interview_answer_question"
        ),
        sa.ForeignKeyConstraint(
            ["session_id"], ["interview_sessions.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["question_id"], ["interview_questions.id"], ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_interview_answers_session_id", "interview_answers", ["session_id"]
    )
    op.create_index(
        "ix_interview_answers_question_id", "interview_answers", ["question_id"]
    )
    op.create_table(
        "company_candidates",
        sa.Column("company_id", sa.String(length=36), nullable=True),
        sa.Column("name", sa.String(length=300), nullable=False),
        sa.Column("website", sa.Text(), nullable=True),
        sa.Column("source", sa.String(length=100), nullable=False),
        sa.Column("role_filter", sa.String(length=300), nullable=True),
        sa.Column("location_filter", sa.String(length=300), nullable=True),
        sa.Column("industry_filter", sa.String(length=300), nullable=True),
        sa.Column("evidence", sa.JSON(), nullable=False),
        sa.Column("score", sa.Float(), nullable=True),
        sa.Column(
            "decision",
            sa.String(length=40),
            server_default=sa.text("'pending'"),
            nullable=False,
        ),
        sa.Column("discovered_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["company_id"], ["companies.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_company_candidates_company_id", "company_candidates", ["company_id"]
    )
    op.create_index(
        "ix_company_candidates_decision",
        "company_candidates",
        ["decision", "discovered_at"],
    )
    op.create_index(
        "ix_company_candidates_discovered_at", "company_candidates", ["discovered_at"]
    )
    op.create_table(
        "company_watchlist",
        sa.Column("company_id", sa.String(length=36), nullable=False),
        sa.Column("criteria", sa.JSON(), nullable=False),
        sa.Column("cadence_days", sa.Integer(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("last_scanned_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("company_id", name="uq_company_watchlist_company"),
        sa.ForeignKeyConstraint(["company_id"], ["companies.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_company_watchlist_enabled", "company_watchlist", ["enabled"])


def downgrade() -> None:
    op.drop_table("company_watchlist")
    op.drop_table("company_candidates")
    op.drop_table("interview_answers")
    op.drop_table("interview_sessions")
    op.drop_table("interview_questions")
