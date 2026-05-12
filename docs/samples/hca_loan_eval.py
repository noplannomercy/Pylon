"""
HCA 여신심사 핵심 로직 — LoanEvaluationService
신용등급 산출, DTI 검증, 승인 경로 결정
"""

from dataclasses import dataclass
from enum import Enum


class CreditGrade(Enum):
    GRADE_1 = 1  # 최우량
    GRADE_2 = 2  # 우량
    GRADE_3 = 3  # 일반
    GRADE_4 = 4  # 조건부
    GRADE_5 = 5  # 거절


class ApprovalStatus(Enum):
    APPROVED = "APPROVED"
    CONDITIONAL = "CONDITIONAL"
    REJECTED = "REJECTED"


@dataclass
class LoanApplication:
    applicant_id: str
    credit_score: int       # 신용점수 (300~1000)
    annual_income: int      # 연소득 (원)
    loan_amount: int        # 대출신청액 (원)
    loan_period_months: int # 대출기간 (개월)


@dataclass
class EvaluationResult:
    application_id: str
    credit_grade: CreditGrade
    dti_ratio: float
    dti_limit: float
    approval_status: ApprovalStatus
    requires_joint_approval: bool
    reject_reason: str | None = None


def calculate_credit_grade(credit_score: int) -> CreditGrade:
    """신용점수 → 신용등급 변환"""
    if credit_score >= 900:
        return CreditGrade.GRADE_1
    elif credit_score >= 800:
        return CreditGrade.GRADE_2
    elif credit_score >= 700:
        return CreditGrade.GRADE_3
    elif credit_score >= 600:
        return CreditGrade.GRADE_4
    else:
        return CreditGrade.GRADE_5


def get_dti_limit(annual_income: int, credit_grade: CreditGrade) -> float:
    """
    소득 구간 + 신용등급 기반 DTI 한도 반환.
    연소득 7천만원 초과 → 우대 한도 적용 (정확히 7천만원은 일반 기준).
    """
    high_income_threshold = 70_000_000

    if annual_income > high_income_threshold:
        # 고소득 우대
        if credit_grade in (CreditGrade.GRADE_1, CreditGrade.GRADE_2):
            return 0.60
        elif credit_grade == CreditGrade.GRADE_3:
            return 0.50
        else:
            return 0.40
    else:
        # 일반 기준
        if credit_grade in (CreditGrade.GRADE_1, CreditGrade.GRADE_2):
            return 0.50
        elif credit_grade == CreditGrade.GRADE_3:
            return 0.40
        else:
            return 0.30


def calculate_dti(annual_income: int, loan_amount: int, loan_period_months: int) -> float:
    """DTI = 연간 원리금 상환액 / 연소득"""
    monthly_payment = loan_amount / loan_period_months
    annual_repayment = monthly_payment * 12
    return annual_repayment / annual_income


def determine_approval(
    credit_grade: CreditGrade,
    dti_ratio: float,
    dti_limit: float,
) -> tuple[ApprovalStatus, str | None]:
    """승인 상태 결정. DTI 초과 시 거절, 4등급은 조건부."""
    if dti_ratio > dti_limit:
        return ApprovalStatus.REJECTED, f"DTI {dti_ratio:.2%} > 한도 {dti_limit:.2%}"

    if credit_grade == CreditGrade.GRADE_5:
        return ApprovalStatus.REJECTED, "신용등급 5등급 — 심사 불가"

    if credit_grade == CreditGrade.GRADE_4:
        return ApprovalStatus.CONDITIONAL, None

    return ApprovalStatus.APPROVED, None


class LoanEvaluationService:
    """여신심사 메인 서비스"""

    def evaluate(self, app: LoanApplication) -> EvaluationResult:
        grade = calculate_credit_grade(app.credit_score)
        dti_limit = get_dti_limit(app.annual_income, grade)
        dti = calculate_dti(app.annual_income, app.loan_amount, app.loan_period_months)
        status, reason = determine_approval(grade, dti, dti_limit)

        # 4등급(CONDITIONAL)은 심사역 2인 공동 승인 필요
        requires_joint = status == ApprovalStatus.CONDITIONAL

        return EvaluationResult(
            application_id=app.applicant_id,
            credit_grade=grade,
            dti_ratio=dti,
            dti_limit=dti_limit,
            approval_status=status,
            requires_joint_approval=requires_joint,
            reject_reason=reason,
        )
