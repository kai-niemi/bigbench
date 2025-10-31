/*
    ANLTR4 grammar for conditional rule logic and arithmetic expressions.
    @author Kai Niemi
 */
parser grammar ExpressionParser;

options { tokenVocab = ExpressionLexer; }

// -------------------------------------------------------------
// Parser rules that translates the token stream into structures
// -------------------------------------------------------------

root
    : expr ignore? EOF
    ;

expr
    : conditional_expr
    | arithmetic_expr
    | string_expr
    | literal
    ;

ignore
    : SEMICOLON
    | COMMENT
    ;

conditional_expr
    : IF condition THEN outcome ELSE outcome SEMICOLON?
    ;

condition
    : logical_expr
    ;

outcome
    : arithmetic_expr
    | string_expr
    | literal
    ;

logical_expr
    : logical_expr AND logical_expr                                         # LogicalExpressionAnd
    | logical_expr OR logical_expr                                          # LogicalExpressionOr
    | NOT logical_expr                                                      # LogicalExpressionNot
    | comparison_expr                                                       # ComparisonExpression
    | LPAREN logical_expr RPAREN                                            # LogicalExpressionInParen
    | logical_entity                                                        # LogicalEntity
    ;

logical_entity
    : booleanLiteral
    | identifier
    | function                                                              
    ;

comparison_expr
    : left=comparison_operand op=comp_operator right=comparison_operand     # ComparisonExpressionOperand
    | left=dateLiteral op=comp_operator right=dateLiteral                   # ComparisonExpressionDate
    | left=timeLiteral op=comp_operator right=timeLiteral                   # ComparisonExpressionTime
    | left=dateTimeLiteral op=comp_operator right=dateTimeLiteral           # ComparisonExpressionDateTime
    | left=stringLiteral op=comp_operator right=stringLiteral               # ComparisonExpressionString
    | left=stringLiteral op=IN right=string_list                            # ComparisonExpressionStringList
    | LPAREN comparison_expr RPAREN                                         # ComparisonExpressionParens
    ;

comparison_operand
    : arithmetic_expr
    ;

comp_operator
    : GT
    | GE
    | LT
    | LE
    | EQ
    | NE
    ;

string_list
    : Identifier                                                            # StringListVariable
    | LPAREN stringLiteral (COMMA stringLiteral)* RPAREN                    # StringArgumentList
    ;

literal
    : booleanLiteral
    | decimalLiteral
    | dateLiteral
    | timeLiteral
    | dateTimeLiteral
    | stringLiteral
    ;

arithmetic_expr
    : left=arithmetic_expr POW right=arithmetic_expr                        # ArithmeticPower
    | left=arithmetic_expr operator=(MULT|DIV) right=arithmetic_expr        # ArithmeticMultiplicationOrDivision
    | left=arithmetic_expr operator=(PLUS|MINUS) right=arithmetic_expr      # ArithmeticPlusOrMinus
    | left=arithmetic_expr operator=(MIN|MAX) right=arithmetic_expr         # ArithmeticMinOrMax
    | left=arithmetic_expr MOD right=arithmetic_expr                        # ArithmeticModulus
    | operator=(MINUS|PLUS) right=arithmetic_expr                           # ArithmeticUnaryMinusOrPlus
    | LPAREN inner=arithmetic_expr RPAREN                                   # ArithmeticParentheses
    | numeric_entity                                                        # ArithmeticNumericEntity
    ;

numeric_entity
    : decimalLiteral                                                        # NumericConstant
    | identifier                                                            # NumericVariable
    | function                                                              # NumericFunction
    ;

string_expr
    : left=string_expr operator=PLUS right=string_expr                      # StringPlus
    | LPAREN inner=string_expr RPAREN                                       # StringParentheses
    | string_entity                                                         # StringEntity
    ;

string_entity
    : stringLiteral                                                         # StringConstant
    | decimalLiteral                                                        # StringDecimal
    | identifier                                                            # StringVariable
    | function                                                              # StringFunction
    ;

booleanLiteral
    : BooleanLiteral
    ;

decimalLiteral
    : DecimalLiteral
    ;

stringLiteral
    : StringLiteral
    ;

dateLiteral
    : DatePrefix DateLiteral RBRACKET
    ;

timeLiteral
    : TimePrefix TimeLiteral RBRACKET
    ;

dateTimeLiteral
    : DateTimePrefix DateTimeLiteral RBRACKET
    ;

identifier
    : Identifier
    ;

function
    : Identifier LPAREN functionArguments RPAREN
    ;

functionArguments
    :
    | functionArgument (COMMA functionArgument)*
    ;

functionArgument
    : arithmetic_expr
    | string_expr
    | literal
    ;

