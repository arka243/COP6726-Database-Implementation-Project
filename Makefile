
CC = g++ -O2 -Wno-deprecated

tag = -i
 
ifdef linux
tag = -n
endif

test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o Heap.o Sorted.o DBFile.o Pipe.o BigQ.o RelationalOp.o Function.o Statistics.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o Heap.o Sorted.o DBFile.o Pipe.o BigQ.o RelationalOp.o Function.o Statistics.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o  			 test.o -lfl -lpthread

test.o: test.cc
	$(CC) -g -c test.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc      

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
       
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
       
DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

Heap.o: Heap.cc
	$(CC) -g -c Heap.cc

Sorted.o: Sorted.cc
	$(CC) -g -c Sorted.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

RelOp.o: RelationalOp.cc
	$(CC) -g -c RelationalOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc
       
y.tab.o: Parser.y
	yacc -d Parser.y
	#sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"
	g++ -c y.tab.c
	       
yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"
	g++ -c yyfunc.tab.c
       
lex.yy.o: Lexer.l
	lex Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c


clean:
	rm -f *.o
	rm -f *.out
	rm -f y.tab.*
	rm -f yyfunc.tab.*
	rm -f lex.yy.*
	rm -f lex.yyfunc*


