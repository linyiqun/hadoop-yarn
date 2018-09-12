package org.apache.hadoop.yarn.nodelabels.constraints;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ConstraintNodeLabel;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.nodelabels.RMNodeConstraint;
import org.apache.hadoop.yarn.nodelabels.constraints.ConstraintExpressionList.Operator;
import org.apache.hadoop.yarn.nodelabels.constraints.DoubleConstraintType.DoubleSetConstraintValue;
import org.apache.hadoop.yarn.nodelabels.constraints.LongConstraintType.LongConstraintValue;
import org.apache.hadoop.yarn.nodelabels.constraints.StringConstraintType.StringSetConstraintValue;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestConstraintExpressionParser {
  private static final String CUSTOM_VERSION = "CustomVersion";
  private static final String HAS_GPU = "HAS_GPU";
  private static final String LINUX = "Linux";
  private static final String Windows = "Windows";
  private static final String HAS_SSD = "has_ssd";
  private static final String ARCHITECTURE = "ARCHITECTURE";
  private static final String JDK_TYPE = "JDK_TYPE";
  private static final String NUM_OF_DISKS = "NUM_OF_DISKS";
  private static final String NUM_OF_INTF = "NUM_OF_INTF";
  private static final String GLIBC = "GLIBC";
  private static final String JDK_VERSION = "JDK_VERSION";
  private static Map<String, RMNodeConstraint> constraintLabels;

  @BeforeClass
  public static void initialize() throws IOException {
    constraintLabels = new HashMap<>();

    //defining set of constraints : global constraints
    // Boolean Constraints : HAS_GPU, Linux,
    constraintLabels.put(HAS_GPU.toLowerCase(),
        new RMNodeConstraint(ConstraintNodeLabel.newInstance(
            HAS_GPU.toLowerCase(), ConstraintsUtil.BOOLEAN_CONSTRAINT_TYPE)));
    constraintLabels.put(LINUX.toLowerCase(),
        new RMNodeConstraint(ConstraintNodeLabel.newInstance(
            LINUX.toLowerCase(), ConstraintsUtil.BOOLEAN_CONSTRAINT_TYPE)));
    constraintLabels.put(Windows.toLowerCase(),
        new RMNodeConstraint(ConstraintNodeLabel.newInstance(
            Windows.toLowerCase(), ConstraintsUtil.BOOLEAN_CONSTRAINT_TYPE)));
    constraintLabels.put(HAS_SSD.toLowerCase(),
        new RMNodeConstraint(ConstraintNodeLabel.newInstance(
            HAS_SSD.toLowerCase(), ConstraintsUtil.BOOLEAN_CONSTRAINT_TYPE)));

    // String Constraints : ARCHITECTURE, JDK_TYPE
    constraintLabels.put(ARCHITECTURE.toLowerCase(),
        new RMNodeConstraint(
            ConstraintNodeLabel.newInstance(ARCHITECTURE.toLowerCase(),
                ConstraintsUtil.STRING_CONSTRAINT_TYPE)));
    constraintLabels.put(JDK_TYPE.toLowerCase(),
        new RMNodeConstraint(ConstraintNodeLabel.newInstance(
            JDK_TYPE.toLowerCase(), ConstraintsUtil.STRING_CONSTRAINT_TYPE)));

    // Long Constraints : NUM_OF_DISKS, NUM_OF_INTF,
    constraintLabels.put(NUM_OF_DISKS.toLowerCase(),
        new RMNodeConstraint(ConstraintNodeLabel.newInstance(
            NUM_OF_DISKS.toLowerCase(), ConstraintsUtil.LONG_CONSTRAINT_TYPE)));
    constraintLabels.put(NUM_OF_INTF.toLowerCase(),
        new RMNodeConstraint(ConstraintNodeLabel.newInstance(
            NUM_OF_INTF.toLowerCase(), ConstraintsUtil.LONG_CONSTRAINT_TYPE)));

    // Double Constraints : GLIBC (version), JDK_VERSION
    constraintLabels.put(GLIBC.toLowerCase(),
        new RMNodeConstraint(ConstraintNodeLabel.newInstance(
            GLIBC.toLowerCase(), ConstraintsUtil.DOUBLE_CONSTRAINT_TYPE)));
    constraintLabels.put(JDK_VERSION.toLowerCase(),
        new RMNodeConstraint(
            ConstraintNodeLabel.newInstance(JDK_VERSION.toLowerCase(),
                ConstraintsUtil.DOUBLE_CONSTRAINT_TYPE)));
  }

  @Test
  public void testExpressionWithExists() throws YarnException {
    String expr = "NUM_OF_DISKS";

    ConstraintExpressionParser parser =
        new ConstraintExpressionParser(expr, constraintLabels);
    ConstraintExpressionList actualExpression = parser.parse();
    ConstraintExpressionList expectedExpression =
        new ConstraintExpressionList(Operator.AND,
            new CompareConstraintExpression(NUM_OF_DISKS.toLowerCase(),
                new LongConstraintType(), ExpressionCompareOp.EXISTS, null,
                false));

    Assert.assertEquals(
        "expression parsing with  \"Exists And SubExpr \" is not matching",
        expectedExpression, actualExpression);
  }

  @Test
  public void testExpressionWithNotExists() throws YarnException {
    String expr = "!HAS_GPU";

    ConstraintExpressionParser parser =
        new ConstraintExpressionParser(expr, constraintLabels);
    ConstraintExpressionList actualExpression = parser.parse();
    ConstraintExpressionList expectedExpression =
        new ConstraintExpressionList(Operator.AND,
            new CompareConstraintExpression(HAS_GPU.toLowerCase(),
                new BooleanConstraintType(), ExpressionCompareOp.NOT_EXISTS,
                null, false));

    Assert.assertEquals(
        "expression parsing with  \"Exists And SubExpr \" is not matching",
        expectedExpression, actualExpression);
  }

  @Test
  public void testExpressionWithInAndNotIn() throws YarnException {
    String expr =
        "JDK_TYPE IN (OPEN_JDK, ORACLE_JDK) && GLIBC NOT_IN ( 1.2, 2.0) ";
    Set<String> entries =
        new HashSet<String>(Arrays.asList("open_jdk", "oracle_jdk"));
    Set<Double> gibcEntries = new HashSet<Double>(Arrays.asList(1.2D, 2.0D));
    ConstraintExpressionParser parser =
        new ConstraintExpressionParser(expr, constraintLabels);
    ConstraintExpressionList actualExpression = parser.parse();
    ConstraintExpressionList expectedExpression =
        new ConstraintExpressionList(Operator.AND,
            new CompareConstraintExpression(JDK_TYPE.toLowerCase(),
                new StringConstraintType(), ExpressionCompareOp.IN,
                new StringSetConstraintValue(true, entries)),
            new CompareConstraintExpression(GLIBC.toLowerCase(),
                new DoubleConstraintType(), ExpressionCompareOp.NOT_IN,
                new DoubleSetConstraintValue(false, gibcEntries)));
    Assert.assertEquals("expression parsing with IN and not in is not matching",
        expectedExpression, actualExpression);

    // create the label mapping for the node:
    Map<String, ConstraintValue> nodeConstraintMappings = new HashMap<>();
    ConstraintValue jdkType = new StringConstraintType()
        .getConstraintValue(ExpressionCompareOp.EQUAL);
    jdkType.parseAndInitValue("MY_JDK", 0);
    nodeConstraintMappings.put(JDK_TYPE.toLowerCase(), jdkType);
    ConstraintValue glibcVal = new DoubleConstraintType()
        .getConstraintValue(ExpressionCompareOp.EQUAL);
    glibcVal.parseAndInitValue("1.5", 0);
    nodeConstraintMappings.put(GLIBC.toLowerCase(), glibcVal);

    Assert.assertFalse("Given node's constraints should not match",
        actualExpression.evaluate(nodeConstraintMappings));

    // correct the label
    jdkType.parseAndInitValue("ORACLE_JDK".toLowerCase(), 0);
    Assert.assertTrue(
        "Given node's constraints should match constraint expression",
        actualExpression.evaluate(nodeConstraintMappings));
  }

  @Test
  public void testExpressionWithExistsAndSubExpr() throws YarnException {
    String expr = "(NUM_OF_DISKS >= 3 || HAS_SSD) && !Windows";

    ConstraintExpressionParser parser =
        new ConstraintExpressionParser(expr, constraintLabels);
    ConstraintExpressionList actualExpression = parser.parse();
    ConstraintExpressionList expectedExpression = new ConstraintExpressionList(
        Operator.AND, new ConstraintExpressionList(
            Operator.OR, new CompareConstraintExpression(
                NUM_OF_DISKS.toLowerCase(), new LongConstraintType(),
                ExpressionCompareOp.GREATER_OR_EQUAL,
                new LongConstraintValue(ExpressionCompareOp.GREATER_OR_EQUAL,
                    3)),
            new CompareConstraintExpression(HAS_SSD.toLowerCase(),
                new BooleanConstraintType(), ExpressionCompareOp.EXISTS, null)),
        new CompareConstraintExpression(Windows.toLowerCase(),
            new BooleanConstraintType(), ExpressionCompareOp.NOT_EXISTS, null,
            false));

    Assert.assertEquals(
        "expression parsing with  \"Exists And SubExpr \" is not matching",
        expectedExpression, actualExpression);

    // create the label mapping for the node:
    Map<String, ConstraintValue> nodeConstraintMappings = new HashMap<>();
    ConstraintValue numDisks =
        new LongConstraintType().getConstraintValue(ExpressionCompareOp.EQUAL);
    numDisks.parseAndInitValue("2", 0);
    nodeConstraintMappings.put(NUM_OF_DISKS.toLowerCase().toLowerCase(),
        numDisks);
    ConstraintValue windowsBool = new BooleanConstraintType()
        .getConstraintValue(ExpressionCompareOp.EQUAL);
    nodeConstraintMappings.put(Windows.toLowerCase(), windowsBool);

    Assert.assertFalse("Given node's constraints should not match",
        actualExpression.evaluate(nodeConstraintMappings));

    // correct the label
    numDisks.parseAndInitValue("3", 0);
    nodeConstraintMappings.remove(Windows.toLowerCase());
    nodeConstraintMappings.put(LINUX.toLowerCase(), windowsBool);
    Assert.assertTrue(
        "Given node's constraints should match constraint expression",
        actualExpression.evaluate(nodeConstraintMappings));
  }

  @Test
  public void testExpressionWithCustomType() throws YarnException, IOException {
    // adding custom constraint to the global list of constraints
    constraintLabels.put(CUSTOM_VERSION.toLowerCase(),
        new RMNodeConstraint(ConstraintNodeLabel.newInstance(
            CUSTOM_VERSION.toLowerCase().toLowerCase(),
            CustomVersion.class.getName())));

    ConstraintExpressionParser parser = new ConstraintExpressionParser(
        "CustomVersion == R10M2P24 || CustomVersion > R10M19P2",
        constraintLabels);
    ConstraintExpressionList actualExpression = parser.parse();
    ConstraintExpressionList expectedExpression =
        new ConstraintExpressionList(Operator.OR,
            new CompareConstraintExpression(CUSTOM_VERSION.toLowerCase(),
                new CustomVersion(), ExpressionCompareOp.EQUAL,
                new CustomVersionValue(ExpressionCompareOp.EQUAL, 10, 2, 24)),
            new CompareConstraintExpression(CUSTOM_VERSION.toLowerCase(),
                new CustomVersion(), ExpressionCompareOp.GREATER_THAN,
                new CustomVersionValue(ExpressionCompareOp.GREATER_THAN, 10, 19,
                    2)));

    Assert.assertEquals(
        "expression parsing with  \"Exists And SubExpr \" is not matching",
        expectedExpression, actualExpression);

    // create the label mapping for the node without the mapping for custom
    // constraint label.
    Map<String, ConstraintValue> nodeConstraintMappings = new HashMap<>();
    ConstraintValue windowsBool = new BooleanConstraintType()
        .getConstraintValue(ExpressionCompareOp.EQUAL);
    nodeConstraintMappings.put(Windows.toLowerCase(), windowsBool);

    Assert.assertFalse("Given node's constraints should not match",
        actualExpression.evaluate(nodeConstraintMappings));

    // create the label mapping for the node with custom constraint.
    ConstraintValue customVersion =
        new CustomVersion().getConstraintValue(ExpressionCompareOp.EQUAL);
    customVersion.parseAndInitValue("R11M1P0".toLowerCase(), 0);
    nodeConstraintMappings.put(CUSTOM_VERSION.toLowerCase(), customVersion);
    Assert.assertTrue(
        "Given node's constraints should match constraint expression",
        actualExpression.evaluate(nodeConstraintMappings));
  }

  public static class CustomVersion implements ConstraintType {
    private Set<ExpressionCompareOp> supportedOps =
        new HashSet<>(Arrays.asList(ExpressionCompareOp.NOT_EXISTS,
            ExpressionCompareOp.EXISTS, ExpressionCompareOp.EQUAL,
            ExpressionCompareOp.NOT_EQUAL, ExpressionCompareOp.GREATER_THAN,
            ExpressionCompareOp.LESS_THAN));

    @Override
    public String getConstraintTypeName() {
      return CustomVersion.class.getName();
    }

    @Override
    public Set<ExpressionCompareOp> getSupportedCompareOperation() {
      return supportedOps;
    }

    @Override
    public ConstraintValue getConstraintValue(ExpressionCompareOp compareOp)
        throws YarnException {
      return new CustomVersionValue(compareOp);
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result
          + ((supportedOps == null) ? 0 : supportedOps.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      CustomVersion other = (CustomVersion) obj;
      if (supportedOps == null) {
        if (other.supportedOps != null)
          return false;
      } else if (!supportedOps.equals(other.supportedOps))
        return false;
      return true;
    }
  }

  public static class CustomVersionValue
      implements ConstraintValue, Comparable<CustomVersionValue> {
    private static char MAJOR_CHAR = 'r';
    private static char MINOR_CHAR = 'm';
    private static char PATCH_CHAR = 'p';
    private int majorVerVal = 0;
    private int minorVerVal = 0;
    private int patchVerVal = 0;
    private ExpressionCompareOp compareOp;

    public CustomVersionValue(ExpressionCompareOp compareOp) {
      this.compareOp = compareOp;
    }

    public CustomVersionValue(ExpressionCompareOp compareOp, int majorVerVal,
        int minorVerVal, int patchVerVal) {
      this.compareOp = compareOp;
      this.majorVerVal = majorVerVal;
      this.minorVerVal = minorVerVal;
      this.patchVerVal = patchVerVal;
    }

    @Override
    public int parseAndInitValue(String expr, int offset) throws YarnException {
      int rIndex = expr.indexOf(MAJOR_CHAR, offset);
      int mIndex = expr.indexOf(MINOR_CHAR, rIndex);
      int pIndex = expr.indexOf(PATCH_CHAR, mIndex);
      if (rIndex == -1 || mIndex == -1 || pIndex == -1) {
        throw new YarnException("Improper custom version @ " + offset);
      }
      int endIndex = expr.indexOf(ExpressionParseConstants.SPACE_CHAR, pIndex);
      endIndex = (endIndex == -1) ? expr.length() : endIndex;
      try {
        majorVerVal = Integer.parseInt(expr.substring(rIndex + 1, mIndex));
        minorVerVal = Integer.parseInt(expr.substring(mIndex + 1, pIndex));
        patchVerVal = Integer.parseInt(expr.substring(pIndex + 1, endIndex));
      } catch (NumberFormatException e) {
        throw new YarnException("Improper custom version @ " + offset);
      }
      return endIndex;
    }

    @Override
    public boolean matches(ConstraintValue targetValue) {
      assert (targetValue instanceof CustomVersionValue);
      CustomVersionValue nodeConstraintVal = ((CustomVersionValue) targetValue);
      switch (compareOp) {
      case EQUAL:
        return this.compareTo(nodeConstraintVal) == 0;
      case NOT_EQUAL:
        return this.compareTo(nodeConstraintVal) != 0;
      case GREATER_THAN:
        return this.compareTo(nodeConstraintVal) <= -1;
      case LESS_THAN:
        return this.compareTo(nodeConstraintVal) >= -1;
      default:
        return false;
      }
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result =
          prime * result + ((compareOp == null) ? 0 : compareOp.hashCode());
      result = prime * result + majorVerVal;
      result = prime * result + minorVerVal;
      result = prime * result + patchVerVal;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      CustomVersionValue other = (CustomVersionValue) obj;
      if (compareOp != other.compareOp)
        return false;
      if (majorVerVal != other.majorVerVal)
        return false;
      if (minorVerVal != other.minorVerVal)
        return false;
      if (patchVerVal != other.patchVerVal)
        return false;
      return true;
    }

    @Override
    public int compareTo(CustomVersionValue o) {
      if (majorVerVal < o.majorVerVal) {
        return -1;
      } else if (majorVerVal > o.majorVerVal) {
        return 1;
      } else {
        if (minorVerVal < o.minorVerVal) {
          return -1;
        } else if (minorVerVal > o.minorVerVal) {
          return 1;
        } else {
          return (patchVerVal < o.patchVerVal) ? -1
              : ((patchVerVal == o.patchVerVal) ? 0 : 1);
        }
      }
    }

    @Override
    public String toString() {
      return " r" + majorVerVal + "m" + minorVerVal + "p" + patchVerVal + " ";
    }
  }
}
