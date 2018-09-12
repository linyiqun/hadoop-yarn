package org.apache.hadoop.yarn.nodelabels.constraints;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.nodelabels.RMNodeConstraint;
import org.apache.hadoop.yarn.nodelabels.constraints.ConstraintExpressionList.Operator;

/**
 * This class is responsible for parsing the constraint expression. It parses
 * the expression based on the format " compareExpression [expressionOperator]
 * compareExpression ". ExpressionOperator's supported are "||" / "or" and "&&"
 * / "and". And Compare expression is of the format: "constraintName
 * [compareOperator] constraintValueExpression". compareOperator's which are
 * supported are <code>
 * <li>LESS_THAN, // '<' or 'lt'</li>
 * <li>LESS_OR_EQUAL, // '<=' or 'le'</li>
 * <li>EQUAL, // '==' or 'eq'</li>
 * <li>NOT_EQUAL, // '!=' or 'ne' or 'ene' (for exists but not equal)</li>
 * <li>GREATER_OR_EQUAL, // '>=' or 'ge'</li>
 * <li>GREATER_THAN, // '>' or 'gt'</li>
 * <li>IN, // 'in'</li>
 * <li>NOT_IN, // 'not_in'</li>
 * <li>EXISTS, // no operand only if the name is specified</li>
 * <li>NOT_EXISTS, // '! constrainName'</li> <code>
 * 
 * Space is expected before and after ExpressionOperator and compareOperator.
 * Based on the constraintType of the constraint name, operator and value is
 * validated. If "(" and ")" is specified then the expression within these
 * braces will be evaluated first. Expressions will be evaluated always from
 * left to right.
 * 
 * If just constraintName is specified then "EXISTS" operator is applied on the
 * given constraintName.Expressions will be always evaluated in the lower case,
 * as well the constraintName (like the partitions) also supports the lower case
 * evaluation.
 */
public class ConstraintExpressionParser {

  private enum ParseState {
    PARSING_CONSTRAINT, // parsing constraint name
    PARSING_VALUE, // parsing value part of expression
    PARSING_OP, // parsing operator between expressions
    PARSING_COMPAREOP // parsing comparison operator within a expression
  }

  // Main expression.
  private final String expr;
  // Expression in lower case.
  private final String exprInLowerCase;
  // position from where the current char is read
  private int offset = 0;
  // offset to read full label or value
  private int lvStartOffset = 0;
  private final int exprLength;
  private ParseState currentParseState = ParseState.PARSING_CONSTRAINT;
  // Linked list implemented as a stack.
  private Deque<ConstraintExpressionList> exprListStack = new LinkedList<>();
  private CompareConstraintExpression currentCompareExpression = null;
  private ConstraintExpressionList expressionList = null;
  private Map<String, RMNodeConstraint> constraintLabels;
  private boolean notExistsOperatorIsSet = false;

  public ConstraintExpressionParser(String expression,
      Map<String, RMNodeConstraint> constraintLabels) {
    if (expression != null) {
      // trim it and replace multiple space with single
      expr = expression.trim().replaceAll("\\s", " ");
      exprLength = expr.length();
      exprInLowerCase = expr.toLowerCase();
    } else {
      expr = null;
      exprInLowerCase = null;
      exprLength = 0;
    }
    this.constraintLabels = constraintLabels;
  }

  protected ConstraintExpression getCurrentCompareExp() {
    return currentCompareExpression;
  }

  protected ConstraintExpressionList getExpressionList() {
    return expressionList;
  }

  private void handleSpaceChar() throws YarnException {
    if (currentParseState == ParseState.PARSING_CONSTRAINT) {
      if (lvStartOffset == offset) {
        // did not encounter any label or value
        lvStartOffset++;
        offset++;
        return;
      }
      String constraintName = exprInLowerCase.substring(lvStartOffset, offset);
      if (currentCompareExpression == null) {
        currentCompareExpression =
            createCompareExpression(constraintName, notExistsOperatorIsSet);
      } else {
        // TODO Need to check whether we need to throw exception here.
      }
      currentParseState = ParseState.PARSING_COMPAREOP;
      offset++;
    } else if (currentParseState == ParseState.PARSING_VALUE) {
      if (currentCompareExpression != null) {
        offset =
            currentCompareExpression.initializeValue(exprInLowerCase, ++offset);
        lvStartOffset = offset;
        currentParseState = ParseState.PARSING_OP;
      }
    } else {
      offset++;
    }
  }

  private CompareConstraintExpression createCompareExpression(
      String constraintName, boolean notExistsOperatorIsSet)
      throws YarnException {
    RMNodeConstraint rmConstraintNodeLabel =
        constraintLabels.get(constraintName);
    if (rmConstraintNodeLabel == null) {
      throw new YarnException(
          "Expression contains unrecognised constraintLabel (" + constraintName
              + " at position " + lvStartOffset + " of expression +" + expr);
    }
    CompareConstraintExpression compareConstraintExpression =
        new CompareConstraintExpression(constraintName,
            rmConstraintNodeLabel.getType());
    if (notExistsOperatorIsSet) {
      // in case of not exists
      compareConstraintExpression.setComareOp(ExpressionCompareOp.NOT_EXISTS);
      compareConstraintExpression.setConstraintMustExist(false);
    }
    return compareConstraintExpression;
  }

  private void handleOpeningBracketChar() throws YarnException {
    if (currentParseState != ParseState.PARSING_CONSTRAINT) {
      throw new YarnException("Encountered unexpected opening bracket @ "
          + offset + " while parsing " + expr + ".");
    }
    offset++;
    lvStartOffset = offset;
    exprListStack.push(expressionList);
    expressionList = null;
  }

  private void handleClosingBracketChar() throws YarnException {
    if (currentParseState == ParseState.PARSING_COMPAREOP) {
      throw new YarnException("Encountered unexpected closing bracket at index "
          + offset + " while parsing " + expr + ".");
    }
    if (!exprListStack.isEmpty()) {
      if (currentParseState == ParseState.PARSING_CONSTRAINT) {
        currentCompareExpression = createCompareExpression(
            exprInLowerCase.substring(lvStartOffset, offset),
            notExistsOperatorIsSet);
        currentParseState = ParseState.PARSING_OP;
      } else if (currentParseState == ParseState.PARSING_VALUE) {
        currentParseState = ParseState.PARSING_OP;
      }
      validateAndAddExpression();

      // As bracket is closing, pop the filter list from top of the stack and
      // combine it with current filter list.
      ConstraintExpressionList fList = exprListStack.pop();
      if (fList != null) {
        fList.addExpression(expressionList);
        expressionList = fList;
      }
      offset++;
      lvStartOffset = offset;
    } else {
      throw new YarnException("Encountered unexpected closing "
          + "bracket while parsing " + expr + ".");
    }
  }

  private void parseCompareOp() throws YarnException {
    if (offset + 2 >= exprLength) {
      throw new YarnException("Compare op cannot be parsed for " + expr + ".");
    }
    ExpressionCompareOp compareOp = null;
    boolean constraintExistFlag = true;
    if (exprInLowerCase
        .charAt(offset + 2) == ExpressionParseConstants.SPACE_CHAR) {
      if (exprInLowerCase.startsWith("eq", offset)
          || exprInLowerCase.startsWith("==", offset)) {
        compareOp = ExpressionCompareOp.EQUAL;
      } else if (exprInLowerCase.startsWith("ne", offset)
          || exprInLowerCase.startsWith("!=", offset)) {
        compareOp = ExpressionCompareOp.NOT_EQUAL;
        constraintExistFlag = false;
      } else if (exprInLowerCase.startsWith("lt", offset)) {
        compareOp = ExpressionCompareOp.LESS_THAN;
      } else if (exprInLowerCase.startsWith("le", offset)
          || exprInLowerCase.startsWith("<=", offset)) {
        compareOp = ExpressionCompareOp.LESS_OR_EQUAL;
      } else if (exprInLowerCase.startsWith("gt", offset)) {
        compareOp = ExpressionCompareOp.GREATER_THAN;
      } else if (exprInLowerCase.startsWith("ge", offset)
          || exprInLowerCase.startsWith(">=", offset)) {
        compareOp = ExpressionCompareOp.GREATER_OR_EQUAL;
      } else if (exprInLowerCase.startsWith("in", offset)) {
        compareOp = ExpressionCompareOp.IN;
      }
      offset = offset + 2;
    } else if (exprInLowerCase.startsWith("ene ", offset)) {
      compareOp = ExpressionCompareOp.NOT_EQUAL;
      offset = offset + 3;
    } else if (exprInLowerCase.startsWith("< ", offset)) {
      compareOp = ExpressionCompareOp.LESS_THAN;
      offset = offset + 1;
    } else if (exprInLowerCase.startsWith("> ", offset)) {
      compareOp = ExpressionCompareOp.GREATER_THAN;
      offset = offset + 1;
    } else if (exprInLowerCase.startsWith("not_in ", offset)) {
      compareOp = ExpressionCompareOp.NOT_IN;
      offset = offset + 6;
    }

    if (compareOp == null) {
      throw new YarnException(
          "Compare op cannot be parsed for " + expr + " @ " + offset);
    }
    currentCompareExpression.setComareOp(compareOp);
    currentCompareExpression.setConstraintMustExist(constraintExistFlag);
    lvStartOffset = offset;
    currentParseState = ParseState.PARSING_VALUE;
  }

  private void parseOp(boolean closingBracket) throws YarnException {
    Operator operator = null;
    if (exprInLowerCase.startsWith("|| ", offset)
        || exprInLowerCase.startsWith("or ", offset)) {
      operator = Operator.OR;
      offset = offset + 3;
    } else if (exprInLowerCase.startsWith("and ", offset)) {
      operator = Operator.AND;
      offset = offset + 4;
    } else if (exprInLowerCase.startsWith("&& ", offset)) {
      operator = Operator.AND;
      offset = offset + 3;
    }
    if (operator == null) {
      throw new YarnException("Operator cannot be parsed for " + expr + ".");
    }
    if (expressionList == null) {
      expressionList = new ConstraintExpressionList(operator);
    }

    if (!closingBracket) {
      // if closingbracket is already processed then the existing compare
      // expression would have been already handled
      validateAndAddExpression();
    }

    if (closingBracket || (expressionList.getExpressionList().size() > 1
        && expressionList.getOperator() != operator)) {
      expressionList = new ConstraintExpressionList(operator, expressionList);
    }

    currentCompareExpression = null;
    lvStartOffset = offset;
    currentParseState = ParseState.PARSING_CONSTRAINT;
  }

  public ConstraintExpressionList parse() throws YarnException {
    if (expr == null || exprLength == 0) {
      return null;
    }
    boolean closingBracket = false;
    while (offset < exprLength) {
      char offsetChar = exprInLowerCase.charAt(offset);
      switch (offsetChar) {
      case ExpressionParseConstants.NOT_CHAR:
        if (currentParseState == ParseState.PARSING_CONSTRAINT) {
          // Only when parsing label "!" is supported as not exist operation,
          // in other cases let it go so that it gets validated for other
          // positions
          notExistsOperatorIsSet = true;
          offset++;
          lvStartOffset++;
          break;
        }
      case ExpressionParseConstants.SPACE_CHAR:
        handleSpaceChar();
        break;
      case ExpressionParseConstants.OPENING_BRACKET_CHAR:
        handleOpeningBracketChar();
        break;
      case ExpressionParseConstants.CLOSING_BRACKET_CHAR:
        handleClosingBracketChar();
        closingBracket = true;
        break;
      default: // other characters.
        // Parse based on state.
        if (currentParseState == ParseState.PARSING_COMPAREOP) {
          parseCompareOp();
        } else if (currentParseState == ParseState.PARSING_OP) {
          parseOp(closingBracket);
          closingBracket = false;
        } else {
          // Might be a key or value. Move ahead.
          offset++;
        }
        break;
      }
    }
    if (!exprListStack.isEmpty()) {
      exprListStack.clear();
      throw new YarnException(
          "Encountered improper brackets while " + "parsing " + expr + ".");
    }

    if (expressionList == null
        || expressionList.getExpressionList().isEmpty()) {
      expressionList = new ConstraintExpressionList();
    }

    if (lvStartOffset != offset) {
      String constraintName = exprInLowerCase.substring(lvStartOffset, offset);
      currentCompareExpression =
          createCompareExpression(constraintName, notExistsOperatorIsSet);

    }
    validateAndAddExpression();

    return expressionList;
  }

  private void validateAndAddExpression() throws YarnException {
    if (currentCompareExpression == null
        || !currentCompareExpression.hasValueBeenSet()) {
      // if the value has not been set then throw exception
      throw new YarnException("Either the expression was not identified or not"
          + " set @ " + offset + " for expression expr " + expr);
    }
    expressionList.addExpression(currentCompareExpression);
    currentCompareExpression = null;
  }
}
