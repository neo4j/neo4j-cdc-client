/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
lexer grammar PatternLexer;

SPACE:
   (     '\u0009'
           | '\n' //can't parse this in unicode
           | '\u000B'
           | '\u000C'
           | '\r' //can't parse this in unicode
           | '\u001C'
           | '\u001D'
           | '\u001E'
           | '\u001F'
           | '\u0020'
           | '\u00A0'
           | '\u1680'
           | '\u2000'
           | '\u2001'
           | '\u2002'
           | '\u2003'
           | '\u2004'
           | '\u2005'
           | '\u2006'
           | '\u2007'
           | '\u2008'
           | '\u2009'
           | '\u200A'
           | '\u2028'
           | '\u2029'
           | '\u202F'
           | '\u205F'
           | '\u3000'
       ) -> channel (HIDDEN);

DECIMAL_DOUBLE:
   ([0-9] (INTEGER_PART)* ('_')? '.' (INTEGER_PART)+ (DECIMAL_EXPONENT)? (IDENTIFIER)? | '.' (INTEGER_PART)+ (DECIMAL_EXPONENT)? (IDENTIFIER)? | [0-9] (INTEGER_PART)* DECIMAL_EXPONENT (IDENTIFIER)?);

UNSIGNED_DECIMAL_INTEGER:
   ([1-9] (INTEGER_PART)* (PART_LETTER)* | '0');

fragment DECIMAL_EXPONENT:
   [eE] ([+\-])? (INTEGER_PART)+ (PART_LETTER)*;

fragment INTEGER_PART:
   ('_')? [0-9];

UNSIGNED_HEX_INTEGER:
   '0' [xX] (PART_LETTER)*;

UNSIGNED_OCTAL_INTEGER:
   '0' ('o')? (PART_LETTER)*;

STRING1_OPEN:
   '\'' -> more , mode (STRING1);

STRING2_OPEN:
   '"' -> more , mode (STRING2);

ESCAPED_SYMBOLIC_NAME_OPEN:
   '`' -> more , mode (ESC_SYMB_NAME);

COLON:
   ':';

COMMA:
   ',';

GT:
   '>';

LBRACKET:
   '[';

LCURLY:
   '{';

LPAREN:
   '(';

LT:
   '<';

MINUS:
   '-';

PLUS:
   '+';

RBRACKET:
   ']';

RCURLY:
   '}';

RPAREN:
   ')';

TIMES:
   '*';

TRUE:
   T R U E;

FALSE:
   F A L S E;

INF:
   I N F;

INFINITY:
   I N F I N I T Y;

NAN:
   N A N;

NULL:
   N U L L;

IDENTIFIER:
   LETTER (PART_LETTER)*;

ErrorChar
    : .
    ;

fragment LETTER:
   [A-Z_a-zªµºÀ-ÖØ-öø-ˁˆ-ˑˠ-ˤˬˮͰ-ʹͶ-ͷͺ-ͽͿΆΈ-ΊΌΎ-ΡΣ-ϵϷ-ҁҊ-ԯԱ-Ֆՙՠ-ֈא-תׯ-ײؠ-يٮ-ٯٱ-ۓەۥ-ۦۮ-ۯۺ-ۼۿܐܒ-ܯݍ-ޥޱߊ-ߪߴ-ߵߺࠀ-ࠕࠚࠤࠨࡀ-ࡘࡠ-ࡪࢠ-ࢴࢶ-ࣇऄ-हऽॐक़-ॡॱ-ঀঅ-ঌএ-ঐও-নপ-রলশ-হঽৎড়-ঢ়য়-ৡৰ-ৱৼਅ-ਊਏ-ਐਓ-ਨਪ-ਰਲ-ਲ਼ਵ-ਸ਼ਸ-ਹਖ਼-ੜਫ਼ੲ-ੴઅ-ઍએ-ઑઓ-નપ-રલ-ળવ-હઽૐૠ-ૡૹଅ-ଌଏ-ଐଓ-ନପ-ରଲ-ଳଵ-ହଽଡ଼-ଢ଼ୟ-ୡୱஃஅ-ஊஎ-ஐஒ-கங-சஜஞ-டண-தந-பம-ஹௐఅ-ఌఎ-ఐఒ-నప-హఽౘ-ౚౠ-ౡಀಅ-ಌಎ-ಐಒ-ನಪ-ಳವ-ಹಽೞೠ-ೡೱ-ೲഄ-ഌഎ-ഐഒ-ഺഽൎൔ-ൖൟ-ൡൺ-ൿඅ-ඖක-නඳ-රලව-ෆก-ะา-ำเ-ๆກ-ຂຄຆ-ຊຌ-ຣລວ-ະາ-ຳຽເ-ໄໆໜ-ໟༀཀ-ཇཉ-ཬྈ-ྌက-ဪဿၐ-ၕၚ-ၝၡၥ-ၦၮ-ၰၵ-ႁႎႠ-ჅჇჍა-ჺჼ-ቈቊ-ቍቐ-ቖቘቚ-ቝበ-ኈኊ-ኍነ-ኰኲ-ኵኸ-ኾዀዂ-ዅወ-ዖዘ-ጐጒ-ጕጘ-ፚᎀ-ᎏᎠ-Ᏽᏸ-ᏽᐁ-ᙬᙯ-ᙿᚁ-ᚚᚠ-ᛪᛮ-ᛸᜀ-ᜌᜎ-ᜑᜠ-ᜱᝀ-ᝑᝠ-ᝬᝮ-ᝰក-ឳៗៜᠠ-ᡸᢀ-ᢄᢇ-ᢨᢪᢰ-ᣵᤀ-ᤞᥐ-ᥭᥰ-ᥴᦀ-ᦫᦰ-ᧉᨀ-ᨖᨠ-ᩔᪧᬅ-ᬳᭅ-ᭋᮃ-ᮠᮮ-ᮯᮺ-ᯥᰀ-ᰣᱍ-ᱏᱚ-ᱽᲀ-ᲈᲐ-ᲺᲽ-Ჿᳩ-ᳬᳮ-ᳳᳵ-ᳶᳺᴀ-ᶿḀ-ἕἘ-Ἕἠ-ὅὈ-Ὅὐ-ὗὙὛὝὟ-ώᾀ-ᾴᾶ-ᾼιῂ-ῄῆ-ῌῐ-ΐῖ-Ίῠ-Ῥῲ-ῴῶ-ῼ‿-⁀⁔ⁱⁿₐ-ₜℂℇℊ-ℓℕℙ-ℝℤΩℨK-ℭℯ-ℹℼ-ℿⅅ-ⅉⅎⅠ-ↈⰀ-Ⱞⰰ-ⱞⱠ-ⳤⳫ-ⳮⳲ-ⳳⴀ-ⴥⴧⴭⴰ-ⵧⵯⶀ-ⶖⶠ-ⶦⶨ-ⶮⶰ-ⶶⶸ-ⶾⷀ-ⷆⷈ-ⷎⷐ-ⷖⷘ-ⷞⸯ々-〇〡-〩〱-〵〸-〼ぁ-ゖゝ-ゟァ-ヺー-ヿㄅ-ㄯㄱ-ㆎㆠ-ㆿㇰ-ㇿ㐀-䶿一-鿼ꀀ-ꒌꓐ-ꓽꔀ-ꘌꘐ-ꘟꘪ-ꘫꙀ-ꙮꙿ-ꚝꚠ-ꛯꜗ-ꜟꜢ-ꞈꞋ-ꞿꟂ-ꟊꟵ-ꠁꠃ-ꠅꠇ-ꠊꠌ-ꠢꡀ-ꡳꢂ-ꢳꣲ-ꣷꣻꣽ-ꣾꤊ-ꤥꤰ-ꥆꥠ-ꥼꦄ-ꦲꧏꧠ-ꧤꧦ-ꧯꧺ-ꧾꨀ-ꨨꩀ-ꩂꩄ-ꩋꩠ-ꩶꩺꩾ-ꪯꪱꪵ-ꪶꪹ-ꪽꫀꫂꫛ-ꫝꫠ-ꫪꫲ-ꫴꬁ-ꬆꬉ-ꬎꬑ-ꬖꬠ-ꬦꬨ-ꬮꬰ-ꭚꭜ-ꭩꭰ-ꯢ가-힣ힰ-ퟆퟋ-ퟻ豈-舘並-龎ﬀ-ﬆﬓ-ﬗיִײַ-ﬨשׁ-זּטּ-לּמּנּ-סּףּ-פּצּ-ﮱﯓ-ﴽﵐ-ﶏﶒ-ﷇﷰ-ﷻ︳-︴﹍-﹏ﹰ-ﹴﹶ-ﻼＡ-Ｚ＿ａ-ｚｦ-ﾾￂ-ￇￊ-ￏￒ-ￗￚ-ￜ];

fragment PART_LETTER:
   [ --$0-9A-Z_a-z-¢-¥ª\u00ADµºÀ-ÖØ-öø-ˁˆ-ˑˠ-ˤˬˮ̀-ʹͶ-ͷͺ-ͽͿΆΈ-ΊΌΎ-ΡΣ-ϵϷ-ҁ҃-҇Ҋ-ԯԱ-Ֆՙՠ-ֈ֏֑-ֽֿׁ-ׂׄ-ׇׅא-תׯ-ײ؀-؅؋ؐ-ؚ\u061Cؠ-٩ٮ-ۓە-۝۟-۪ۨ-ۼۿ܏-݊ݍ-ޱ߀-ߵߺ߽-࠭ࡀ-࡛ࡠ-ࡪࢠ-ࢴࢶ-ࣇ࣓-ॣ०-९ॱ-ঃঅ-ঌএ-ঐও-নপ-রলশ-হ়-ৄে-ৈো-ৎৗড়-ঢ়য়-ৣ০-৳৻-ৼ৾ਁ-ਃਅ-ਊਏ-ਐਓ-ਨਪ-ਰਲ-ਲ਼ਵ-ਸ਼ਸ-ਹ਼ਾ-ੂੇ-ੈੋ-੍ੑਖ਼-ੜਫ਼੦-ੵઁ-ઃઅ-ઍએ-ઑઓ-નપ-રલ-ળવ-હ઼-ૅે-ૉો-્ૐૠ-ૣ૦-૯૱ૹ-૿ଁ-ଃଅ-ଌଏ-ଐଓ-ନପ-ରଲ-ଳଵ-ହ଼-ୄେ-ୈୋ-୍୕-ୗଡ଼-ଢ଼ୟ-ୣ୦-୯ୱஂ-ஃஅ-ஊஎ-ஐஒ-கங-சஜஞ-டண-தந-பம-ஹா-ூெ-ைொ-்ௐௗ௦-௯௹ఀ-ఌఎ-ఐఒ-నప-హఽ-ౄె-ైొ-్ౕ-ౖౘ-ౚౠ-ౣ౦-౯ಀ-ಃಅ-ಌಎ-ಐಒ-ನಪ-ಳವ-ಹ಼-ೄೆ-ೈೊ-್ೕ-ೖೞೠ-ೣ೦-೯ೱ-ೲഀ-ഌഎ-ഐഒ-ൄെ-ൈൊ-ൎൔ-ൗൟ-ൣ൦-൯ൺ-ൿඁ-ඃඅ-ඖක-නඳ-රලව-ෆ්ා-ුූෘ-ෟ෦-෯ෲ-ෳก-ฺ฿-๎๐-๙ກ-ຂຄຆ-ຊຌ-ຣລວ-ຽເ-ໄໆ່-ໍ໐-໙ໜ-ໟༀ༘-༙༠-༩༹༵༷༾-ཇཉ-ཬཱ-྄྆-ྗྙ-ྼ࿆က-၉ၐ-ႝႠ-ჅჇჍა-ჺჼ-ቈቊ-ቍቐ-ቖቘቚ-ቝበ-ኈኊ-ኍነ-ኰኲ-ኵኸ-ኾዀዂ-ዅወ-ዖዘ-ጐጒ-ጕጘ-ፚ፝-፟ᎀ-ᎏᎠ-Ᏽᏸ-ᏽᐁ-ᙬᙯ-ᙿᚁ-ᚚᚠ-ᛪᛮ-ᛸᜀ-ᜌᜎ-᜔ᜠ-᜴ᝀ-ᝓᝠ-ᝬᝮ-ᝰᝲ-ᝳក-៓ៗ៛-៝០-៩᠋-᠎᠐-᠙ᠠ-ᡸᢀ-ᢪᢰ-ᣵᤀ-ᤞᤠ-ᤫᤰ-᤻᥆-ᥭᥰ-ᥴᦀ-ᦫᦰ-ᧉ᧐-᧙ᨀ-ᨛᨠ-ᩞ᩠-᩿᩼-᪉᪐-᪙ᪧ᪰-᪽ᪿ-ᫀᬀ-ᭋ᭐-᭙᭫-᭳ᮀ-᯳ᰀ-᰷᱀-᱉ᱍ-ᱽᲀ-ᲈᲐ-ᲺᲽ-Ჿ᳐-᳔᳒-ᳺᴀ-᷹᷻-ἕἘ-Ἕἠ-ὅὈ-Ὅὐ-ὗὙὛὝὟ-ώᾀ-ᾴᾶ-ᾼιῂ-ῄῆ-ῌῐ-ΐῖ-Ίῠ-Ῥῲ-ῴῶ-ῼ​-‏‪-‮‿-⁀⁔⁠-⁤⁦-⁯ⁱⁿₐ-ₜ₠-₿⃐-⃥⃜⃡-⃰ℂℇℊ-ℓℕℙ-ℝℤΩℨK-ℭℯ-ℹℼ-ℿⅅ-ⅉⅎⅠ-ↈⰀ-Ⱞⰰ-ⱞⱠ-ⳤⳫ-ⳳⴀ-ⴥⴧⴭⴰ-ⵧⵯ⵿-ⶖⶠ-ⶦⶨ-ⶮⶰ-ⶶⶸ-ⶾⷀ-ⷆⷈ-ⷎⷐ-ⷖⷘ-ⷞⷠ-ⷿⸯ々-〇〡-〯〱-〵〸-〼ぁ-ゖ゙-゚ゝ-ゟァ-ヺー-ヿㄅ-ㄯㄱ-ㆎㆠ-ㆿㇰ-ㇿ㐀-䶿一-鿼ꀀ-ꒌꓐ-ꓽꔀ-ꘌꘐ-ꘫꙀ-꙯ꙴ-꙽ꙿ-꛱ꜗ-ꜟꜢ-ꞈꞋ-ꞿꟂ-ꟊꟵ-ꠧ꠬꠸ꡀ-ꡳꢀ-ꣅ꣐-꣙꣠-ꣷꣻꣽ-꤭ꤰ-꥓ꥠ-ꥼꦀ-꧀ꧏ-꧙ꧠ-ꧾꨀ-ꨶꩀ-ꩍ꩐-꩙ꩠ-ꩶꩺ-ꫂꫛ-ꫝꫠ-ꫯꫲ-꫶ꬁ-ꬆꬉ-ꬎꬑ-ꬖꬠ-ꬦꬨ-ꬮꬰ-ꭚꭜ-ꭩꭰ-ꯪ꯬-꯭꯰-꯹가-힣ힰ-ퟆퟋ-ퟻ豈-舘並-龎ﬀ-ﬆﬓ-ﬗיִ-ﬨשׁ-זּטּ-לּמּנּ-סּףּ-פּצּ-ﮱﯓ-ﴽﵐ-ﶏﶒ-ﷇﷰ-﷼︀-️︠-︯︳-︴﹍-﹏﹩ﹰ-ﹴﹶ-ﻼ\uFEFF＄０-９Ａ-Ｚ＿ａ-ｚｦ-ﾾￂ-ￇￊ-ￏￒ-ￗￚ-ￜ￠-￡￥-￦￹-￻];

ARROW_LINE:
   [\-\u00AD‐‑‒–—―﹘﹣－];

ARROW_LEFT_HEAD:
   [⟨〈﹤＜];

ARROW_RIGHT_HEAD:
   [⟩〉﹥＞];

MORE2:
   . -> more;

mode STRING1;
MORE3:
   '\\\\' -> more;

MORE4:
   '\\\'' -> more;

MORE5:
   '\\"' -> more;

MORE6:
   '\\b' -> more;

MORE7:
   '\\f' -> more;

MORE8:
   '\\n' -> more;

MORE9:
   '\\r' -> more;

MORE10:
   '\\t' -> more;

MORE11:
   '\\u[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]' -> more;

MORE12:
   ~ ['] -> more;

STRING_LITERAL1:
   '\'' -> mode (DEFAULT_MODE);

mode STRING2;
MORE13:
   '\\\\' -> more;

MORE14:
   '\\\'' -> more;

MORE15:
   '\\"' -> more;

MORE16:
   '\\b' -> more;

MORE17:
   '\\f' -> more;

MORE18:
   '\\n' -> more;

MORE19:
   '\\r' -> more;

MORE20:
   '\\t' -> more;

MORE21:
   '\\u[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]' -> more;

MORE22:
   ~ ["] -> more;

STRING_LITERAL2:
   '"' -> mode (DEFAULT_MODE);

mode ESC_SYMB_NAME;
MORE23:
   ~ [`] -> more;

MORE24:
   '``' -> more;

ESCAPED_SYMBOLIC_NAME:
   '`' -> mode (DEFAULT_MODE);

fragment A:
   [aA];

fragment B:
   [bB];

fragment C:
   [cC];

fragment D:
   [dD];

fragment E:
   [eE];

fragment F:
   [fF];

fragment G:
   [gG];

fragment H:
   [hH];

fragment I:
   [iI];

fragment J:
   [jJ];

fragment K:
   [kK];

fragment L:
   [lL];

fragment M:
   [mM];

fragment N:
   [nN];

fragment O:
   [oO];

fragment P:
   [pP];

fragment Q:
   [qQ];

fragment R:
   [rR];

fragment S:
   [sS];

fragment T:
   [tT];

fragment U:
   [uU];

fragment V:
   [vV];

fragment W:
   [wW];

fragment X:
   [xX];

fragment Y:
   [yY];

fragment Z:
   [zZ];

