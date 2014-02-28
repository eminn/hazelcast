/* Generated By:JavaCC: Do not edit this line. SQLParserTokenManager.java */
package com.hazelcast.query.parser;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import java.io.StringReader;
import java.io.Reader;
import java.util.ArrayList;

/** Token Manager. */
public class SQLParserTokenManager implements SQLParserConstants
{

  /** Debug output. */
  public  java.io.PrintStream debugStream = System.out;
  /** Set debug output. */
  public  void setDebugStream(java.io.PrintStream ds) { debugStream = ds; }
private final int jjStopStringLiteralDfa_0(int pos, long active0)
{
   switch (pos)
   {
      case 0:
         if ((active0 & 0x1001feL) != 0L)
         {
            jjmatchedKind = 22;
            return 6;
         }
         return -1;
      case 1:
         if ((active0 & 0x14L) != 0L)
            return 6;
         if ((active0 & 0x1001eaL) != 0L)
         {
            jjmatchedKind = 22;
            jjmatchedPos = 1;
            return 6;
         }
         return -1;
      case 2:
         if ((active0 & 0x1001e0L) != 0L)
         {
            jjmatchedKind = 22;
            jjmatchedPos = 2;
            return 6;
         }
         if ((active0 & 0xaL) != 0L)
            return 6;
         return -1;
      case 3:
         if ((active0 & 0x100040L) != 0L)
            return 6;
         if ((active0 & 0x1a0L) != 0L)
         {
            jjmatchedKind = 22;
            jjmatchedPos = 3;
            return 6;
         }
         return -1;
      case 4:
         if ((active0 & 0x180L) != 0L)
            return 6;
         if ((active0 & 0x20L) != 0L)
         {
            jjmatchedKind = 22;
            jjmatchedPos = 4;
            return 6;
         }
         return -1;
      case 5:
         if ((active0 & 0x20L) != 0L)
         {
            jjmatchedKind = 22;
            jjmatchedPos = 5;
            return 6;
         }
         return -1;
      default :
         return -1;
   }
}
private final int jjStartNfa_0(int pos, long active0)
{
   return jjMoveNfa_0(jjStopStringLiteralDfa_0(pos, active0), pos + 1);
}
private int jjStopAtPos(int pos, int kind)
{
   jjmatchedKind = kind;
   jjmatchedPos = pos;
   return pos + 1;
}
private int jjMoveStringLiteralDfa0_0()
{
   switch(curChar)
   {
      case 33:
         return jjMoveStringLiteralDfa1_0(0x1000L);
      case 40:
         return jjStopAtPos(0, 17);
      case 41:
         return jjStopAtPos(0, 18);
      case 44:
         return jjStopAtPos(0, 30);
      case 60:
         jjmatchedKind = 14;
         return jjMoveStringLiteralDfa1_0(0x10000L);
      case 62:
         jjmatchedKind = 13;
         return jjMoveStringLiteralDfa1_0(0x8000L);
      case 65:
      case 97:
         return jjMoveStringLiteralDfa1_0(0x2L);
      case 66:
      case 98:
         return jjMoveStringLiteralDfa1_0(0x20L);
      case 73:
      case 105:
         return jjMoveStringLiteralDfa1_0(0x90L);
      case 76:
      case 108:
         return jjMoveStringLiteralDfa1_0(0x40L);
      case 78:
      case 110:
         return jjMoveStringLiteralDfa1_0(0x100008L);
      case 79:
      case 111:
         return jjMoveStringLiteralDfa1_0(0x4L);
      case 82:
      case 114:
         return jjMoveStringLiteralDfa1_0(0x100L);
      default :
         return jjMoveNfa_0(0, 0);
   }
}
private int jjMoveStringLiteralDfa1_0(long active0)
{
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(0, active0);
      return 1;
   }
   switch(curChar)
   {
      case 61:
         if ((active0 & 0x1000L) != 0L)
            return jjStopAtPos(1, 12);
         else if ((active0 & 0x8000L) != 0L)
            return jjStopAtPos(1, 15);
         else if ((active0 & 0x10000L) != 0L)
            return jjStopAtPos(1, 16);
         break;
      case 69:
      case 101:
         return jjMoveStringLiteralDfa2_0(active0, 0x120L);
      case 73:
      case 105:
         return jjMoveStringLiteralDfa2_0(active0, 0x40L);
      case 76:
      case 108:
         return jjMoveStringLiteralDfa2_0(active0, 0x80L);
      case 78:
      case 110:
         if ((active0 & 0x10L) != 0L)
            return jjStartNfaWithStates_0(1, 4, 6);
         return jjMoveStringLiteralDfa2_0(active0, 0x2L);
      case 79:
      case 111:
         return jjMoveStringLiteralDfa2_0(active0, 0x8L);
      case 82:
      case 114:
         if ((active0 & 0x4L) != 0L)
            return jjStartNfaWithStates_0(1, 2, 6);
         break;
      case 85:
      case 117:
         return jjMoveStringLiteralDfa2_0(active0, 0x100000L);
      default :
         break;
   }
   return jjStartNfa_0(0, active0);
}
private int jjMoveStringLiteralDfa2_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(0, old0);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(1, active0);
      return 2;
   }
   switch(curChar)
   {
      case 68:
      case 100:
         if ((active0 & 0x2L) != 0L)
            return jjStartNfaWithStates_0(2, 1, 6);
         break;
      case 71:
      case 103:
         return jjMoveStringLiteralDfa3_0(active0, 0x100L);
      case 73:
      case 105:
         return jjMoveStringLiteralDfa3_0(active0, 0x80L);
      case 75:
      case 107:
         return jjMoveStringLiteralDfa3_0(active0, 0x40L);
      case 76:
      case 108:
         return jjMoveStringLiteralDfa3_0(active0, 0x100000L);
      case 84:
      case 116:
         if ((active0 & 0x8L) != 0L)
            return jjStartNfaWithStates_0(2, 3, 6);
         return jjMoveStringLiteralDfa3_0(active0, 0x20L);
      default :
         break;
   }
   return jjStartNfa_0(1, active0);
}
private int jjMoveStringLiteralDfa3_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(1, old0);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(2, active0);
      return 3;
   }
   switch(curChar)
   {
      case 69:
      case 101:
         if ((active0 & 0x40L) != 0L)
            return jjStartNfaWithStates_0(3, 6, 6);
         return jjMoveStringLiteralDfa4_0(active0, 0x100L);
      case 75:
      case 107:
         return jjMoveStringLiteralDfa4_0(active0, 0x80L);
      case 76:
      case 108:
         if ((active0 & 0x100000L) != 0L)
            return jjStartNfaWithStates_0(3, 20, 6);
         break;
      case 87:
      case 119:
         return jjMoveStringLiteralDfa4_0(active0, 0x20L);
      default :
         break;
   }
   return jjStartNfa_0(2, active0);
}
private int jjMoveStringLiteralDfa4_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(2, old0);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(3, active0);
      return 4;
   }
   switch(curChar)
   {
      case 69:
      case 101:
         if ((active0 & 0x80L) != 0L)
            return jjStartNfaWithStates_0(4, 7, 6);
         return jjMoveStringLiteralDfa5_0(active0, 0x20L);
      case 88:
      case 120:
         if ((active0 & 0x100L) != 0L)
            return jjStartNfaWithStates_0(4, 8, 6);
         break;
      default :
         break;
   }
   return jjStartNfa_0(3, active0);
}
private int jjMoveStringLiteralDfa5_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(3, old0);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(4, active0);
      return 5;
   }
   switch(curChar)
   {
      case 69:
      case 101:
         return jjMoveStringLiteralDfa6_0(active0, 0x20L);
      default :
         break;
   }
   return jjStartNfa_0(4, active0);
}
private int jjMoveStringLiteralDfa6_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(4, old0);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(5, active0);
      return 6;
   }
   switch(curChar)
   {
      case 78:
      case 110:
         if ((active0 & 0x20L) != 0L)
            return jjStartNfaWithStates_0(6, 5, 6);
         break;
      default :
         break;
   }
   return jjStartNfa_0(5, active0);
}
private int jjStartNfaWithStates_0(int pos, int kind, int state)
{
   jjmatchedKind = kind;
   jjmatchedPos = pos;
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) { return pos + 1; }
   return jjMoveNfa_0(state, pos + 1);
}
static final long[] jjbitVec0 = {
   0x0L, 0x0L, 0xffffffffffffffffL, 0xffffffffffffffffL
};
private int jjMoveNfa_0(int startState, int curPos)
{
   int startsAt = 0;
   jjnewStateCnt = 30;
   int i = 1;
   jjstateSet[0] = startState;
   int kind = 0x7fffffff;
   for (;;)
   {
      if (++jjround == 0x7fffffff)
         ReInitRounds();
      if (curChar < 64)
      {
         long l = 1L << curChar;
         do
         {
            switch(jjstateSet[--i])
            {
               case 0:
                  if ((0x3ff000000000000L & l) != 0L)
                  {
                     if (kind > 21)
                        kind = 21;
                     jjCheckNAddStates(0, 4);
                  }
                  else if (curChar == 46)
                     jjCheckNAdd(14);
                  else if (curChar == 34)
                     jjCheckNAdd(11);
                  else if (curChar == 39)
                     jjCheckNAdd(8);
                  else if (curChar == 45)
                     jjCheckNAdd(4);
                  else if (curChar == 61)
                     jjstateSet[jjnewStateCnt++] = 1;
                  if (curChar == 61)
                  {
                     if (kind > 9)
                        kind = 9;
                  }
                  break;
               case 1:
                  if (curChar == 61 && kind > 9)
                     kind = 9;
                  break;
               case 2:
                  if (curChar == 61)
                     jjstateSet[jjnewStateCnt++] = 1;
                  break;
               case 3:
                  if (curChar == 45)
                     jjCheckNAdd(4);
                  break;
               case 4:
                  if ((0x3ff000000000000L & l) == 0L)
                     break;
                  if (kind > 21)
                     kind = 21;
                  jjCheckNAdd(4);
                  break;
               case 6:
                  if ((0x3ff400000000000L & l) == 0L)
                     break;
                  if (kind > 22)
                     kind = 22;
                  jjstateSet[jjnewStateCnt++] = 6;
                  break;
               case 7:
                  if (curChar == 39)
                     jjCheckNAdd(8);
                  break;
               case 8:
                  if ((0xffffff7fffffffffL & l) != 0L)
                     jjCheckNAddTwoStates(8, 9);
                  break;
               case 9:
                  if (curChar == 39 && kind > 23)
                     kind = 23;
                  break;
               case 10:
                  if (curChar == 34)
                     jjCheckNAdd(11);
                  break;
               case 11:
                  if ((0xfffffffbffffffffL & l) != 0L)
                     jjCheckNAddTwoStates(11, 12);
                  break;
               case 12:
                  if (curChar == 34 && kind > 24)
                     kind = 24;
                  break;
               case 13:
                  if (curChar == 46)
                     jjCheckNAdd(14);
                  break;
               case 14:
                  if ((0x3ff000000000000L & l) == 0L)
                     break;
                  if (kind > 25)
                     kind = 25;
                  jjCheckNAddTwoStates(14, 15);
                  break;
               case 16:
                  if ((0x280000000000L & l) != 0L)
                     jjCheckNAdd(17);
                  break;
               case 17:
                  if ((0x3ff000000000000L & l) == 0L)
                     break;
                  if (kind > 25)
                     kind = 25;
                  jjCheckNAdd(17);
                  break;
               case 18:
                  if ((0x3ff000000000000L & l) == 0L)
                     break;
                  if (kind > 21)
                     kind = 21;
                  jjCheckNAddStates(0, 4);
                  break;
               case 19:
                  if ((0x3ff000000000000L & l) == 0L)
                     break;
                  if (kind > 21)
                     kind = 21;
                  jjCheckNAdd(19);
                  break;
               case 20:
                  if ((0x3ff000000000000L & l) != 0L)
                     jjCheckNAddTwoStates(20, 21);
                  break;
               case 21:
                  if (curChar != 46)
                     break;
                  if (kind > 25)
                     kind = 25;
                  jjCheckNAddTwoStates(22, 23);
                  break;
               case 22:
                  if ((0x3ff000000000000L & l) == 0L)
                     break;
                  if (kind > 25)
                     kind = 25;
                  jjCheckNAddTwoStates(22, 23);
                  break;
               case 24:
                  if ((0x280000000000L & l) != 0L)
                     jjCheckNAdd(25);
                  break;
               case 25:
                  if ((0x3ff000000000000L & l) == 0L)
                     break;
                  if (kind > 25)
                     kind = 25;
                  jjCheckNAdd(25);
                  break;
               case 26:
                  if ((0x3ff000000000000L & l) != 0L)
                     jjCheckNAddTwoStates(26, 27);
                  break;
               case 28:
                  if ((0x280000000000L & l) != 0L)
                     jjCheckNAdd(29);
                  break;
               case 29:
                  if ((0x3ff000000000000L & l) == 0L)
                     break;
                  if (kind > 25)
                     kind = 25;
                  jjCheckNAdd(29);
                  break;
               default : break;
            }
         } while(i != startsAt);
      }
      else if (curChar < 128)
      {
         long l = 1L << (curChar & 077);
         do
         {
            switch(jjstateSet[--i])
            {
               case 0:
               case 6:
                  if ((0x7fffffe87fffffeL & l) == 0L)
                     break;
                  if (kind > 22)
                     kind = 22;
                  jjCheckNAdd(6);
                  break;
               case 8:
                  jjAddStates(5, 6);
                  break;
               case 11:
                  jjAddStates(7, 8);
                  break;
               case 15:
                  if ((0x2000000020L & l) != 0L)
                     jjAddStates(9, 10);
                  break;
               case 23:
                  if ((0x2000000020L & l) != 0L)
                     jjAddStates(11, 12);
                  break;
               case 27:
                  if ((0x2000000020L & l) != 0L)
                     jjAddStates(13, 14);
                  break;
               default : break;
            }
         } while(i != startsAt);
      }
      else
      {
         int i2 = (curChar & 0xff) >> 6;
         long l2 = 1L << (curChar & 077);
         do
         {
            switch(jjstateSet[--i])
            {
               case 8:
                  if ((jjbitVec0[i2] & l2) != 0L)
                     jjAddStates(5, 6);
                  break;
               case 11:
                  if ((jjbitVec0[i2] & l2) != 0L)
                     jjAddStates(7, 8);
                  break;
               default : break;
            }
         } while(i != startsAt);
      }
      if (kind != 0x7fffffff)
      {
         jjmatchedKind = kind;
         jjmatchedPos = curPos;
         kind = 0x7fffffff;
      }
      ++curPos;
      if ((i = jjnewStateCnt) == (startsAt = 30 - (jjnewStateCnt = startsAt)))
         return curPos;
      try { curChar = input_stream.readChar(); }
      catch(java.io.IOException e) { return curPos; }
   }
}
static final int[] jjnextStates = {
   19, 20, 21, 26, 27, 8, 9, 11, 12, 16, 17, 24, 25, 28, 29, 
};

/** Token literal values. */
public static final String[] jjstrLiteralImages = {
"", null, null, null, null, null, null, null, null, null, null, null, 
"\41\75", "\76", "\74", "\76\75", "\74\75", "\50", "\51", null, null, null, null, null, 
null, null, null, null, null, null, "\54", };

/** Lexer state names. */
public static final String[] lexStateNames = {
   "DEFAULT",
};
static final long[] jjtoToken = {
   0x43f7f3ffL, 
};
static final long[] jjtoSkip = {
   0x3c000000L, 
};
protected SimpleCharStream input_stream;
private final int[] jjrounds = new int[30];
private final int[] jjstateSet = new int[60];
protected char curChar;
/** Constructor. */
public SQLParserTokenManager(SimpleCharStream stream){
   if (SimpleCharStream.staticFlag)
      throw new Error("ERROR: Cannot use a static CharStream class with a non-static lexical analyzer.");
   input_stream = stream;
}

/** Constructor. */
public SQLParserTokenManager(SimpleCharStream stream, int lexState){
   this(stream);
   SwitchTo(lexState);
}

/** Reinitialise parser. */
public void ReInit(SimpleCharStream stream)
{
   jjmatchedPos = jjnewStateCnt = 0;
   curLexState = defaultLexState;
   input_stream = stream;
   ReInitRounds();
}
private void ReInitRounds()
{
   int i;
   jjround = 0x80000001;
   for (i = 30; i-- > 0;)
      jjrounds[i] = 0x80000000;
}

/** Reinitialise parser. */
public void ReInit(SimpleCharStream stream, int lexState)
{
   ReInit(stream);
   SwitchTo(lexState);
}

/** Switch to specified lex state. */
public void SwitchTo(int lexState)
{
   if (lexState >= 1 || lexState < 0)
      throw new TokenMgrError("Error: Ignoring invalid lexical state : " + lexState + ". State unchanged.", TokenMgrError.INVALID_LEXICAL_STATE);
   else
      curLexState = lexState;
}

protected Token jjFillToken()
{
   final Token t;
   final String curTokenImage;
   final int beginLine;
   final int endLine;
   final int beginColumn;
   final int endColumn;
   String im = jjstrLiteralImages[jjmatchedKind];
   curTokenImage = (im == null) ? input_stream.GetImage() : im;
   beginLine = input_stream.getBeginLine();
   beginColumn = input_stream.getBeginColumn();
   endLine = input_stream.getEndLine();
   endColumn = input_stream.getEndColumn();
   t = Token.newToken(jjmatchedKind, curTokenImage);

   t.beginLine = beginLine;
   t.endLine = endLine;
   t.beginColumn = beginColumn;
   t.endColumn = endColumn;

   return t;
}

int curLexState = 0;
int defaultLexState = 0;
int jjnewStateCnt;
int jjround;
int jjmatchedPos;
int jjmatchedKind;

/** Get the next Token. */
public Token getNextToken() 
{
  Token matchedToken;
  int curPos = 0;

  EOFLoop :
  for (;;)
  {
   try
   {
      curChar = input_stream.BeginToken();
   }
   catch(java.io.IOException e)
   {
      jjmatchedKind = 0;
      matchedToken = jjFillToken();
      return matchedToken;
   }

   try { input_stream.backup(0);
      while (curChar <= 32 && (0x100002600L & (1L << curChar)) != 0L)
         curChar = input_stream.BeginToken();
   }
   catch (java.io.IOException e1) { continue EOFLoop; }
   jjmatchedKind = 0x7fffffff;
   jjmatchedPos = 0;
   curPos = jjMoveStringLiteralDfa0_0();
   if (jjmatchedKind != 0x7fffffff)
   {
      if (jjmatchedPos + 1 < curPos)
         input_stream.backup(curPos - jjmatchedPos - 1);
      if ((jjtoToken[jjmatchedKind >> 6] & (1L << (jjmatchedKind & 077))) != 0L)
      {
         matchedToken = jjFillToken();
         return matchedToken;
      }
      else
      {
         continue EOFLoop;
      }
   }
   int error_line = input_stream.getEndLine();
   int error_column = input_stream.getEndColumn();
   String error_after = null;
   boolean EOFSeen = false;
   try { input_stream.readChar(); input_stream.backup(1); }
   catch (java.io.IOException e1) {
      EOFSeen = true;
      error_after = curPos <= 1 ? "" : input_stream.GetImage();
      if (curChar == '\n' || curChar == '\r') {
         error_line++;
         error_column = 0;
      }
      else
         error_column++;
   }
   if (!EOFSeen) {
      input_stream.backup(1);
      error_after = curPos <= 1 ? "" : input_stream.GetImage();
   }
   throw new TokenMgrError(EOFSeen, curLexState, error_line, error_column, error_after, curChar, TokenMgrError.LEXICAL_ERROR);
  }
}

private void jjCheckNAdd(int state)
{
   if (jjrounds[state] != jjround)
   {
      jjstateSet[jjnewStateCnt++] = state;
      jjrounds[state] = jjround;
   }
}
private void jjAddStates(int start, int end)
{
   do {
      jjstateSet[jjnewStateCnt++] = jjnextStates[start];
   } while (start++ != end);
}
private void jjCheckNAddTwoStates(int state1, int state2)
{
   jjCheckNAdd(state1);
   jjCheckNAdd(state2);
}

private void jjCheckNAddStates(int start, int end)
{
   do {
      jjCheckNAdd(jjnextStates[start]);
   } while (start++ != end);
}

}
