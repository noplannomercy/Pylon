CREATE OR REPLACE PACKAGE BODY LOAN_EVAL_PKG AS

  -- 여신 등급 계산: 신용 점수 기반 1~5 등급 반환
  FUNCTION GET_CREDIT_GRADE(p_score IN NUMBER) RETURN NUMBER IS
    v_grade NUMBER;
  BEGIN
    IF p_score >= 900 THEN
      v_grade := 1;
    ELSIF p_score >= 800 THEN
      v_grade := 2;
    ELSIF p_score >= 700 THEN
      v_grade := 3;
    ELSIF p_score >= 600 THEN
      v_grade := 4;
    ELSE
      v_grade := 5;
    END IF;
    RETURN v_grade;
  END GET_CREDIT_GRADE;

  -- DTI 검증: 한도 초과 여부 반환 (Y/N)
  FUNCTION VALIDATE_DTI(
    p_annual_income IN NUMBER,
    p_total_debt_payment IN NUMBER
  ) RETURN VARCHAR2 IS
    v_dti NUMBER;
    v_limit NUMBER := 0.4; -- 기본 40%
  BEGIN
    IF p_annual_income > 70000000 THEN
      v_limit := 0.5; -- 고소득자 50%
    END IF;
    v_dti := p_total_debt_payment / NULLIF(p_annual_income, 0);
    IF v_dti <= v_limit THEN
      RETURN 'Y';
    ELSE
      RETURN 'N';
    END IF;
  END VALIDATE_DTI;

  -- 여신 심사 메인: 신청 승인 여부 결정
  PROCEDURE EVALUATE_LOAN(
    p_cust_id      IN VARCHAR2,
    p_credit_score IN NUMBER,
    p_annual_income IN NUMBER,
    p_loan_amount  IN NUMBER,
    p_result       OUT VARCHAR2,
    p_reason       OUT VARCHAR2
  ) IS
    v_grade   NUMBER;
    v_dti_ok  VARCHAR2(1);
  BEGIN
    v_grade  := GET_CREDIT_GRADE(p_credit_score);
    v_dti_ok := VALIDATE_DTI(p_annual_income, p_loan_amount * 0.12); -- 연 상환액 추정

    IF v_grade = 5 THEN
      p_result := 'REJECT';
      p_reason := '신용 등급 5등급 이하 - 원칙적 불가';
    ELSIF v_dti_ok = 'N' THEN
      p_result := 'REJECT';
      p_reason := 'DTI 한도 초과';
    ELSIF v_grade = 4 THEN
      p_result := 'CONDITIONAL';
      p_reason := '4등급 - 심사역 2인 공동 승인 필요';
    ELSE
      p_result := 'APPROVE';
      p_reason := '심사 통과 (등급: ' || v_grade || ')';
    END IF;

    INSERT INTO LOAN_EVAL_LOG (CUST_ID, CREDIT_SCORE, GRADE, LOAN_AMOUNT, RESULT, REASON, EVAL_DT)
    VALUES (p_cust_id, p_credit_score, v_grade, p_loan_amount, p_result, p_reason, SYSDATE);
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      p_result := 'ERROR';
      p_reason := SQLERRM;
      ROLLBACK;
  END EVALUATE_LOAN;

END LOAN_EVAL_PKG;
/
