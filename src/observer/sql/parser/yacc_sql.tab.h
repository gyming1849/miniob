/* A Bison parser, made by GNU Bison 3.5.1.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2020 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* Undocumented macros, especially those whose name start with YY_,
   are private implementation details.  Do not rely on them.  */

#ifndef YY_YY_YACC_SQL_TAB_H_INCLUDED
# define YY_YY_YACC_SQL_TAB_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int yydebug;
#endif

/* Token type.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    SEMICOLON = 258,
    CREATE = 259,
    DROP = 260,
    TABLE = 261,
    TABLES = 262,
    INDEX = 263,
    SELECT = 264,
    SHOW = 265,
    SYNC = 266,
    INSERT = 267,
    DELETE = 268,
    UPDATE = 269,
    LBRACE = 270,
    RBRACE = 271,
    COMMA = 272,
    TRX_BEGIN = 273,
    TRX_COMMIT = 274,
    TRX_ROLLBACK = 275,
    INT_T = 276,
    STRING_T = 277,
    FLOAT_T = 278,
    HELP = 279,
    EXIT = 280,
    DOT = 281,
    INTO = 282,
    VALUES = 283,
    FROM = 284,
    WHERE = 285,
    AND = 286,
    SET = 287,
    ON = 288,
    LOAD = 289,
    DATA = 290,
    INFILE = 291,
    EQ = 292,
    LT = 293,
    GT = 294,
    LE = 295,
    GE = 296,
    NE = 297,
    GROUP = 298,
    ORDER = 299,
    COUNT = 300,
    AVG = 301,
    SUM = 302,
    MIN = 303,
    MAX = 304,
    ASC = 305,
    DESC = 306,
    BY = 307,
    NUMBER = 308,
    FLOAT = 309,
    ID = 310,
    PATH = 311,
    SSS = 312,
    STAR = 313,
    STRING_V = 314
  };
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 116 "yacc_sql.y"

  struct _Attr *attr;
  struct _Condition *condition1;
  struct _Value *value1;
  char *string;
  int number;
  float floats;
	char *position;

#line 127 "yacc_sql.tab.h"

};
typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif



int yyparse (void *scanner);

#endif /* !YY_YY_YACC_SQL_TAB_H_INCLUDED  */
