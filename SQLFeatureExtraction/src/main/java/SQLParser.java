import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeSet;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
//import net.sf.jsqlparser.statement.select.Union;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.update.Update;
import toolsForMetrics.ColumnExpressionVisitor;
import toolsForMetrics.ExtendedColumn;
import toolsForMetrics.Global;
import toolsForMetrics.Operation;
import toolsForMetrics.Schema;
import toolsForMetrics.SelectItemListParser;
import toolsForMetrics.Util;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
//import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

public class SQLParser{
	String originalSQL;
	String rewrittenSQL;
	SchemaParser schParse; // used for retrieval of schema related information
	List<Table> tables = new ArrayList<Table>();
	List<Table> curLevelTables = new ArrayList<Table>();
	HashSet<Column> groupByColumns = new HashSet<Column>();
	HashSet<Column> selectionColumns = new HashSet<Column>();
	HashSet<Column> havingColumns = new HashSet<Column>();
	HashSet<Column> orderByColumns = new HashSet<Column>();
	HashSet<Column> projectionColumns = new HashSet<Column>();
	HashSet<ArrayList<Column>> joinPredicates = new HashSet<ArrayList<Column>>();
	HashSet<String> limitList = new HashSet<String>();
	HashSet<Column> MINColumns = new HashSet<Column>();
	HashSet<Column> MAXColumns = new HashSet<Column>();
	HashSet<Column> AVGColumns = new HashSet<Column>();
	HashSet<Column> SUMColumns = new HashSet<Column>();
	HashSet<Column> COUNTColumns = new HashSet<Column>();
	HashMap<String, Column> colAliases = new HashMap<String, Column>();
	boolean includeSelOpConst;
	String dataset;
	HashMap<Column,ArrayList<String>> selPredOps;
	HashMap<Column,ArrayList<String>> selPredConstants;
	
	public HashMap<String, Column> getColAliases() {
		return this.colAliases;
	}
	
	public HashMap<Column,ArrayList<String>> getSelPredConstants() {
		return this.selPredConstants;
	}
	
	public HashMap<Column,ArrayList<String>> getSelPredOps() {
		return this.selPredOps;
	}
	
	public List<Table> getTables(){
		return this.tables;
	}
	
	public HashSet<Column> getGroupByColumns(){
		return this.groupByColumns;
	}
	
	public HashSet<Column> getOrderByColumns(){
		return this.orderByColumns;
	}
	
	public HashSet<Column> getHavingColumns(){
		return this.havingColumns;
	}
	
	public HashSet<Column> getProjectionColumns(){
		return this.projectionColumns;
	}
	
	public HashSet<Column> getSelectionColumns(){
		return this.selectionColumns;
	}
	
	public HashSet<ArrayList<Column>> getJoinPredicates(){
		return this.joinPredicates;
	}
	
	public HashSet<String> getLimitList(){
		return this.limitList;
	}
	
	public HashSet<Column> getMINColumns(){
		return this.MINColumns;
	}
	
	public HashSet<Column> getMAXColumns(){
		return this.MAXColumns;
	}
	
	public HashSet<Column> getAVGColumns(){
		return this.AVGColumns;
	}
	
	public HashSet<Column> getSUMColumns(){
		return this.SUMColumns;
	}
	
	public HashSet<Column> getCOUNTColumns(){
		return this.COUNTColumns;
	}
	
	public SQLParser(SchemaParser schParse, String originalSQL, boolean includeSelOpConst, String dataset) {
		this.schParse = schParse;
		this.originalSQL = originalSQL;
		this.includeSelOpConst = includeSelOpConst;
		this.dataset = dataset;
		if(this.includeSelOpConst) {
			this.selPredOps = new HashMap<Column, ArrayList<String>>();
			this.selPredConstants = new HashMap<Column, ArrayList<String>>();
		}
	}
	
	private void consumeTable(Table t) {
		// uppercase it
		t.setName(t.getName().toUpperCase());			
		// if there is any alias of this table,create a key to map to original table
		if (t.getAlias() != null) {
			//t.setAlias(t.getName());
			Global.tableAlias.put(t.getAlias(), t.getName());
		}
		if (t.getAlias() == null) {
			Iterator<Entry<String, String>> it = Global.tableAlias.entrySet().iterator();
			while(it.hasNext()) {
				Entry<String, String> temp = it.next();
				if (temp.getValue().equals(t.getName())) {
					t.setAlias(temp.getKey());
				}
			}
		}
		tables.add(t);		
	}
	
	private void consumeFromItem(FromItem fromitem, List<Table> tables, int queryOrder) {
		
		if (fromitem instanceof Table) {
			Table t = (Table) fromitem;
			// uppercase it
			t.setName(t.getName().replace("`","").toUpperCase());
			
			// if there is any alias of this table,create a key to map to original table
			if (t.getAlias() != null) {
				//t.setAlias(t.getName());
				Global.tableAlias.put(t.getAlias(), t.getName());
			}
			if (t.getAlias() == null) {
				Iterator<Entry<String, String>> it = Global.tableAlias.entrySet().iterator();
				while(it.hasNext()) {
					Entry<String, String> temp = it.next();
					if (temp.getValue().equals(t.getName())) {
						t.setAlias(temp.getKey());
					}
				}
			}

			tables.add(t);
			this.curLevelTables.add(t);
		} else if (fromitem instanceof SubSelect){
			SubSelect temp = (SubSelect) fromitem;
			
			executeSelectWithAlias(temp.getSelectBody(), queryOrder, temp.getAlias()); // recursively passes the alias down into the subselect
		}
	}
	
	private void parseGroupBy(List<Expression> groupbyRef) {
		if (groupbyRef != null) {
			// pop out the top iter
			for (int i = 0; i < groupbyRef.size(); i++) {
				this.correct(groupbyRef.get(i), tables);
				//breaking selection operators with AND
				List<Expression> columns = Util.processSelect(groupbyRef.get(i));
				for (int j = 0; j < columns.size(); j++) {
					Schema groupBySchema = Util.processExpression(columns.get(j));
					for (int k = 0; k < groupBySchema.getValues().size(); k++) {
						groupByColumns.add(new ExtendedColumn(groupBySchema.getValues().get(k)));
						
					}
				}
			}
		}
	}
	
	public Column searchForSelPredOpConstCol(Column selectionColumn, HashMap<Column,ArrayList<String>> selPredOpConsts) {
		for(Column col : selPredOpConsts.keySet()) {
			if(col.toString().equals(selectionColumn.toString())){
				return col;
			}
		}
		return null;
	}
	
	public void setSelPredOpConst(HashMap<Column,ArrayList<String>> selPredOpConsts, Column selectionColumn, String OpConst) {
		Column selCol = searchForSelPredOpConstCol(selectionColumn, selPredOpConsts);
		if(selCol == null) {
			selCol = selectionColumn;
			selPredOpConsts.put(selCol, new ArrayList<String>());
		}
		ArrayList<String> opConstList = selPredOpConsts.get(selCol);
		opConstList.add(OpConst);
		selPredOpConsts.put(selCol, opConstList);
		return;
	}
	
	public Column getColumn(Schema schema) {
		ExtendedColumn selectionColumn = null; 
		for (int j = 0; j < schema.getValues().size(); j++) {
			selectionColumn = new ExtendedColumn(schema.getValues().get(j)); // there is only one column
		}
		return selectionColumn;
	}
	
	public void addSelectionPredicate(Schema leftSchema, BinaryExpression whereExp) {
		ExtendedColumn selectionColumn = (ExtendedColumn)getColumn(leftSchema); 
		if(selectionColumn ==null)
			return;
		this.selectionColumns.add(selectionColumn);
		if(!this.includeSelOpConst)
			return;
		String Op = whereExp.getStringExpression();
		
		String rightExpStr = whereExp.getRightExpression().toString();
		if(rightExpStr.contains("IS NULL")) {
			setSelPredOpConst(this.selPredOps, selectionColumn, "=");
			setSelPredOpConst(this.selPredConstants, selectionColumn, "NULL");
		} else if(rightExpStr.contains("IS NOT NULL")) {
			setSelPredOpConst(this.selPredOps, selectionColumn, "<>");
			setSelPredOpConst(this.selPredConstants, selectionColumn, "NULL");
		} else if(rightExpStr.contains("LIKE")) {
			setSelPredOpConst(this.selPredOps, selectionColumn, "LIKE");
			setSelPredOpConst(this.selPredConstants, selectionColumn, rightExpStr.split(" ")[rightExpStr.split(" ").length-1]);
		} else {
			setSelPredOpConst(this.selPredOps, selectionColumn, Op);
			setSelPredOpConst(this.selPredConstants, selectionColumn, rightExpStr);
		}
	}
	
	public void addSelJoinPredicate(Schema leftSchema, Schema rightSchema, BinaryExpression whereExp) {
		ExtendedColumn leftColumn = (ExtendedColumn)getColumn(leftSchema);
		if(leftColumn ==null)
			return;
		String rightExpStr = whereExp.getRightExpression().toString();
		String Op = whereExp.getStringExpression();
		if(rightExpStr.contains("IS NULL")) {
			this.selectionColumns.add(leftColumn);
			if(this.includeSelOpConst) {
				setSelPredOpConst(this.selPredOps, leftColumn, "=");
				setSelPredOpConst(this.selPredConstants, leftColumn, "NULL");
			}
		} else if(rightExpStr.contains("IS NOT NULL")) {
			this.selectionColumns.add(leftColumn);
			if(this.includeSelOpConst) {
				setSelPredOpConst(this.selPredOps, leftColumn, "<>");
				setSelPredOpConst(this.selPredConstants, leftColumn, "NULL");
			}
		} else if(rightExpStr.contains("LIKE")) {
			this.selectionColumns.add(leftColumn);
			if(this.includeSelOpConst) {
				setSelPredOpConst(this.selPredOps, leftColumn, "LIKE");
				setSelPredOpConst(this.selPredConstants, leftColumn, rightExpStr.split(" ")[rightExpStr.split(" ").length-1]);
			}
		} else if(rightExpStr.contains("\"")) {
			this.selectionColumns.add(leftColumn);
			if(this.includeSelOpConst) {
				setSelPredOpConst(this.selPredOps, leftColumn, Op);
				setSelPredOpConst(this.selPredConstants, leftColumn, rightExpStr.split(" ")[rightExpStr.split(" ").length-1]);
			}
		}
		else {
			ArrayList<Column> joinPredicate = new ArrayList<Column>();
			ExtendedColumn rightColumn = (ExtendedColumn)getColumn(rightSchema);
			joinPredicate.add(leftColumn);
			joinPredicate.add(rightColumn);
			this.joinPredicates.add(joinPredicate);
		}
	}
	
	
	public void parseSelJoinPredsWithConstants(List<Expression> whereExps) {
		for(Expression whereExp : whereExps) {
			if(whereExp instanceof BinaryExpression) {
				Schema leftSchema = Util.processExpression(((BinaryExpression)whereExp).getLeftExpression());
				Schema rightSchema = Util.processExpression(((BinaryExpression)whereExp).getRightExpression()); 
				if(whereExp instanceof Multiplication || whereExp instanceof Addition || whereExp instanceof Subtraction || whereExp instanceof Division) {
					List<Expression> localWhereExps = new ArrayList<Expression>();
					localWhereExps.add(((BinaryExpression)whereExp).getLeftExpression());
					localWhereExps.add(((BinaryExpression)whereExp).getRightExpression());
					parseSelJoinPredsWithConstants(localWhereExps);
				}
				else if(leftSchema.getValues().size() > 0 && rightSchema.getValues().size() == 0) {
					addSelectionPredicate(leftSchema, (BinaryExpression)whereExp);
				}
				else {
					addSelJoinPredicate(leftSchema, rightSchema, (BinaryExpression)whereExp);
				}
			} 
			else {
				Schema leftSchema = Util.processExpression(whereExp);
				if(leftSchema.getValues().size() > 0) {
					ExtendedColumn leftColumn = (ExtendedColumn)getColumn(leftSchema);
					if(leftColumn ==null)
						return;
					this.selectionColumns.add(leftColumn);
					if(this.includeSelOpConst && whereExp.toString().contains("IS NULL")) {
						setSelPredOpConst(this.selPredOps, leftColumn, "=");
						setSelPredOpConst(this.selPredConstants, leftColumn, "NULL");
					} else if(this.includeSelOpConst && whereExp.toString().contains("IS NOT NULL")) {
						//this.selectionColumns.add(leftColumn);
						setSelPredOpConst(this.selPredOps, leftColumn, "<>");
						setSelPredOpConst(this.selPredConstants, leftColumn, "NULL");
					} 
				}
			}
		}
	}
	
	public List<Expression> processSelectWithConstants(Expression e) 
	{
		List<Expression> retVal = new ArrayList<Expression>();

		if (e instanceof Parenthesis) {
			retVal.addAll(processSelectWithConstants(Util.deleteParanthesis(e)));
		} /*else if (e instanceof ExistsExpression) {
			retVal.addAll(processSelectWithConstants(((ExistsExpression) e).getRightExpression()));
		}*/ else if (e instanceof BinaryExpression){
			BinaryExpression a = (BinaryExpression)e;
			Expression leftExp = a.getLeftExpression();
			Expression rightExp = a.getRightExpression();
			if (leftExp instanceof Column && Util.isColValInstance(rightExp)) {
				retVal.add(e);
			}
			else {
				retVal.addAll(processSelectWithConstants(a.getLeftExpression()));
				retVal.addAll(processSelectWithConstants(a.getRightExpression()));
			}
		} 
		/*
		else if (e instanceof SubSelect) {
			ColumnExpressionVisitor visitor = new ColumnExpressionVisitor(); 
	        e.accept(visitor); 
	        
	        retVal.addAll(visitor.getColumns());
		} else if (e instanceof InExpression) {
			InExpression a = (InExpression)e;
			retVal.addAll(processSelectWithConstants(a.getLeftExpression()));
			
			ItemsList it = a.getItemsList();
			if (it != null) {
				ColumnExpressionVisitor visitor = new ColumnExpressionVisitor();
				it.accept(visitor);
				retVal.addAll(visitor.getColumns());
			}
		} */
		else {
			retVal.add(e);
		}
		
		return retVal;
	}
	
	
	private void parseWhereOpsWithConstants(Expression where) {
		if (where != null) {
			// pop out the top iter
			this.correct(where, tables);
			//breaking selection operators with AND
			List<Expression> whereExps = this.processSelectWithConstants(where);
			// for instance, table.col1=table2.col2 will produce a column and a column, whereas table.col1=constant will produce a column and a constant
			parseSelJoinPredsWithConstants(whereExps);			
		}
	}
	
	private void parseWhere(Expression where) {
		// This is the only valid code for with and without constants
		parseWhereOpsWithConstants(where);
	}
	
	private void parseJoinList(List<Join> joinlist, int queryOrder) {
		// This is the only valid code for with and without constants
		parseJoinListOpsWithSelPredConstants(joinlist, queryOrder);
	}
	
	private void parseUsingColumns(List<Join> joinlist, int i) {
		List<Column> columns = joinlist.get(i).getUsingColumns(); //USING clause is used to match the same column name from two diff tables
		if (columns != null) {
			for (int j = 0; j < columns.size(); j++) {
				//System.out.println(columns.get(j).toString());
				Column column=(Column) columns.get(j);
				if (column.getTable()==null||column.getTable().getName()==null){				  					
					Column v=Global.getColumnFullName(column,tables);
					column.setTable(v.getTable());
					column.setColumnName(v.getColumnName());
				}
				else{				  
					String tablename = column.getTable().getName();
					Table t = new Table();
					t.setName(tablename.toUpperCase());
					column.setTable(t);
				}
				
				//System.out.println(column.toString());
				ArrayList<Column> joinPredicate = new ArrayList<Column>();
				for(int numCol=0; numCol<2; numCol++) {
					joinPredicate.add(new ExtendedColumn(column)); // add the same column twice -- [col1, col1]
				}
				this.joinPredicates.add(joinPredicate);
			}
		}
		return;
	}
	
	private void parseJoinListOpsWithSelPredConstants(List<Join> joinlist, int queryOrder) {
		for (int i = 0; i < joinlist.size(); i++) {
			consumeFromItem(joinlist.get(i).getRightItem(), tables, queryOrder);
			
			Expression sss = joinlist.get(i).getOnExpression();
			//System.out.println(sss);
			if (sss != null) {
				// pop out the top iter
				this.correct(sss, tables);
				//breaking selection operators with AND
				List<Expression> joins = Util.processSelectWithConstants(sss);
				parseSelJoinPredsWithConstants(joins);
				/*ArrayList<Column> joinPredicate = null;
				for (int j = 0; j < joins.size(); j++) {
					if (j%2==0) {
						if(joinPredicate != null && joinPredicate.size() == 2)
							joinPredicates.add(joinPredicate);
						else if(joinPredicate != null && joinPredicate.size() == 1)
							selectionColumns.add(joinPredicate.get(0));
						joinPredicate = new ArrayList<Column>();
					}
					Schema joinSchema = Util.processExpression(joins.get(j));
					for (int k = 0; k < joinSchema.getValues().size(); k++) {							
						joinPredicate.add(new ExtendedColumn(joinSchema.getValues().get(k)));
					}
				}
				if(joinPredicate != null && joinPredicate.size() == 2)
					joinPredicates.add(joinPredicate);
				else if(joinPredicate != null && joinPredicate.size() == 1)
					selectionColumns.add(joinPredicate.get(0)); */
			}
			parseUsingColumns(joinlist, i);		
		}
	}
	
	//find all columns with no table name or aliased table name,make them toUppercase
	public void correct(Expression exp, List<Table> tables){
		if (exp instanceof Function){
			Function f=(Function)exp;
			f.setName(f.getName().toUpperCase());
			if (!f.isAllColumns())
			{
				ExpressionList explist= f.getParameters();
				if (explist != null) {
					List<Expression> list=explist.getExpressions();
					for (int i=0;i<list.size();i++)
						correct(list.get(i), tables);
				}
			}
		}

		else if (exp instanceof Column){
			Column c=(Column) exp;
			if (c.getTable()==null||c.getTable().getName()==null){				  					
				Column v=Global.getColumnFullName(c,tables);
				c.setTable(v.getTable());
				c.setColumnName(v.getColumnName());
			}
			else{				  
				String tablename = c.getTable().getName();
				Table t = new Table();
				t.setName(tablename.toUpperCase());
				c.setTable(t);
			}
		}
		//for any other expression with more than one elements
		else if (exp instanceof BinaryExpression){
			BinaryExpression bexp=(BinaryExpression) exp;
			Expression l=bexp.getLeftExpression();
			Expression r=bexp.getRightExpression();
			correct(l,tables);
			correct(r,tables);
		}
		else if (exp instanceof Parenthesis) {
			Parenthesis p=(Parenthesis) exp;
			Expression exp1=p.getExpression();
			correct(exp1,tables);
		}
		else if (exp instanceof CaseExpression){
			CaseExpression c=(CaseExpression) exp;
			List<WhenClause> e=c.getWhenClauses();
			for (int i=0;i<e.size();i++){
				WhenClause clause=e.get(i);
				correct(clause.getThenExpression(),tables);
				correct(clause.getWhenExpression(),tables);
			}
		}
		else if(exp instanceof InExpression) {
			InExpression e = (InExpression) exp;
			parseInExpression(e);
		}
		else if(exp instanceof ExistsExpression) {
			ExistsExpression e = (ExistsExpression) exp;
			correct(e.getRightExpression(), tables);
		}
		else if (exp instanceof SubSelect){
			//subselects.add((SubSelect)exp);
			SubSelect e = (SubSelect) exp;
			PlainSelect sel = (PlainSelect)(e.getSelectBody());
			SelectBody selectItem = e.getSelectBody();
			this.executeSelect(selectItem, 1);
			//correct(sel.getWhere(), tables);
		}
		else {
			//do sth
		}
	}
	
	public List<Expression> parseInAsJoinExp(InExpression e, List<Expression> whereExps) {
		SubSelect subExp = (SubSelect)e.getItemsList();
		PlainSelect subSelExp = (PlainSelect)subExp.getSelectBody();
		SelectExpressionItem subSelProjColItem = (SelectExpressionItem)subSelExp.getSelectItems().get(0);
		Expression subSelProjCol = subSelProjColItem.getExpression();
		if(subSelProjColItem.getAlias()==null) {
			// find the table
			FromItem fromitem = subSelExp.getFromItem();
			if (fromitem instanceof Table) {
				Table subSelTable = (Table) fromitem;
				String tabName = subSelTable.getName();
				subSelTable.setName(tabName.replace("`","").toLowerCase());
				BinaryExpression joinExp;
				if(e.isNot())
					joinExp = new NotEqualsTo();
				else
					joinExp = new EqualsTo();
				joinExp.setLeftExpression(e.getLeftExpression());
				Column c = (Column)subSelProjCol;
				if(c.getTable().getName()==null || c.getTable().getAlias()==null)
					c.setTable(subSelTable);
				if (subSelProjCol != null) {
					joinExp.setRightExpression(subSelProjCol);
					whereExps.add(joinExp);
				}
			}
		}
		return whereExps;
	}
	
	public List<Expression> parseInAsSelExp(InExpression e, List<Expression> whereExps) {
		Expression leftExp = e.getLeftExpression();
		ItemsList it = e.getItemsList();
		String[] constList = it.toString().split(",");
		for(String constStr : constList) {
			BinaryExpression selExp;
			if(e.isNot())
				selExp = new NotEqualsTo();
			else
				selExp = new EqualsTo();
			selExp.setLeftExpression(leftExp);
			StringValue val = new StringValue(constStr);
			selExp.setRightExpression(val);
			whereExps.add(selExp);
		}
		return whereExps;
	}
	
	public void parseInExpression(InExpression e) {
		List<Expression> whereExps = new ArrayList<Expression>();
		correct(e.getLeftExpression(), tables);
		if(!(e.getItemsList() instanceof ExpressionList)) {
			correct((Expression)e.getItemsList(), tables);
			whereExps = parseInAsJoinExp(e, whereExps);
		} else {
			whereExps = parseInAsSelExp(e, whereExps);
			InExpression a = (InExpression)e;
		}
		parseSelJoinPredsWithConstants(whereExps);
	}
	
	public void addToColSet(Schema selectSchema, String colAlias, HashSet<Column> targetColumns) {
		for (int j = 0; j < selectSchema.getValues().size(); j++) {
			Column aggrCol = new ExtendedColumn(selectSchema.getValues().get(j));
			//aggrColumns.add(aggrCol);
			if(aggrCol.getTable().getAlias()==null && aggrCol.getTable().getName()==null) {
				for(Table t : this.curLevelTables) {
					HashMap<String,String> schTabCols = this.schParse.fetchMINCColumns();
					String candTabName = t.getName().toLowerCase().replace("`", "");
					String candColName = aggrCol.getColumnName().toLowerCase().replace("`", "");
					String candColArrayStr = schTabCols.get(candTabName);
					List<String> candColList = null;
					try {
						String[] candColArr = MINCFragmentIntent.cleanColArrayString(candColArrayStr);
						candColList = Arrays.asList(candColArr);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						//e.printStackTrace();
						return;
					}
					if(candColList.contains(candColName)) {
						//t.setName(t.getName().toLowerCase());
						//aggrCol.setTable(t);
						//targetColumns.add(aggrCol);
						Column extendedCol = new ExtendedColumn(t.getName().toLowerCase().replace("`", "")+"."+aggrCol.getColumnName().toLowerCase().replace("`", ""));
						targetColumns.add(extendedCol);
						if(colAlias !=null)
							this.colAliases.put(t.getName().toLowerCase()+"."+colAlias, extendedCol);
					}
				}
			} else {
				targetColumns.add(aggrCol);
			}
		}
		return;
	}
	
	
	public void addColumnToAggrProj(Expression sss, HashSet<Column> aggrColumns, Schema selectSchema, String colAlias) {
		if (((Function) sss).isAllColumns()) {
			for(Table t : this.curLevelTables) {
				Column extendedCol = new ExtendedColumn(t.getName().toLowerCase().replace("`", "")+".*");
				aggrColumns.add(extendedCol);
				projectionColumns.add(extendedCol);
				if(colAlias !=null)
					this.colAliases.put(t.getName().toLowerCase().replace("`", "")+"."+colAlias, extendedCol);
			}
		} else {
			addToColSet(selectSchema, colAlias, aggrColumns);
		}
		return;
	}
	
	
	
	/**
	 *  specifies how to execute Select,major part
	 * @param s
	 */
	@SuppressWarnings("unchecked")
	private void executePlainSelect(PlainSelect s, int queryOrder) {
		//Column c;
		
		//	List<Schema> schemas = collectQueryItems(s);
		
		
		// do check based on their priority to nest iterators
		// 1.first check things from the FROM CLAUSE

		FromItem fromitem = s.getFromItem();
		this.curLevelTables.clear();

		if (fromitem != null) {
			List<Join> joinlist = s.getJoins();
			if (joinlist == null || joinlist.isEmpty()) {
				//consumeFromItem(fromitem, tables, schemas);
				consumeFromItem(fromitem, tables, queryOrder);
			}
			// if multiple tables
			else {
				//consumeFromItem(fromitem, tables, schemas);
				consumeFromItem(fromitem, tables, queryOrder);
				parseJoinList(joinlist, queryOrder);
				//Collections.sort(this.scanList);
			}
			
		} else
			System.err.println("no from item found,please check");
		
		List<SelectItem> selectItems = s.getSelectItems();
//		SelectItemListParser parser = new SelectItemListParser(selectItems, tables);

		if (selectItems != null) {
			// check whether there is any function
//			int flip = 0;
			for (int i = 0; i < selectItems.size(); i++) {
				SelectItem ss = selectItems.get(i);
				if (ss instanceof SelectExpressionItem) {
					Expression sss = ((SelectExpressionItem) ss)
							.getExpression();
					String colAlias = ((SelectExpressionItem) ss).getAlias();
					if (sss instanceof SubSelect) {
						SubSelect temp = (SubSelect) sss;
						
						executeSelect(temp.getSelectBody(), queryOrder);
					}
					Schema selectSchema = Util.processExpression(sss);
					if (sss instanceof Function){
						Function f=(Function)sss;  		
						String fName=f.getName();
						if (fName.equals("max"))
							addColumnToAggrProj(sss, MAXColumns, selectSchema, colAlias);
						else if(fName.equals("min"))
							addColumnToAggrProj(sss, MINColumns, selectSchema, colAlias);
						else if (fName.equals("sum"))
							addColumnToAggrProj(sss, SUMColumns, selectSchema, colAlias);
						else if (fName.equals("avg"))
							addColumnToAggrProj(sss, AVGColumns, selectSchema, colAlias);
						else if (fName.equals("count"))
							addColumnToAggrProj(sss, COUNTColumns, selectSchema, colAlias);
					}
					addToColSet(selectSchema, colAlias, this.projectionColumns); //both non-aggregate & aggregate columns are also added to the projection list
				}
				else if (ss instanceof SubSelect) {
					SubSelect temp = (SubSelect) ss;
					
					executeSelect(temp.getSelectBody(), queryOrder);
				}
				else if (ss instanceof AllColumns){
					//String colAlias = ((SelectExpressionItem) ss).getAlias();
					for(Table t : this.curLevelTables) {
						Column extendedCol = new ExtendedColumn(t.getName().toLowerCase()+".*");
						projectionColumns.add(extendedCol);
						/*if(colAlias !=null)
							this.colAliases.put(colAlias, extendedCol);*/
					}
				}
				else if (ss instanceof AllTableColumns) {
					//String colAlias = ((SelectExpressionItem) ss).getAlias();
					Column extendedCol = new ExtendedColumn(ss.toString());
					projectionColumns.add(extendedCol);
					/*if(colAlias !=null)
						this.colAliases.put(colAlias, extendedCol);*/
				}
				else {
					//System.out.println(ss);
				}
			}
		}

		// 2.check where condition and do selection and join-predicate extraction
		Expression where = s.getWhere();
		parseWhere(where);

		// 3.1 check group by and corresponding aggregation
		List<Expression> groupbyRef = s.getGroupByColumnReferences();
		parseGroupBy(groupbyRef);
		
		// 3.2 check order by and corresponding aggregation
		List<OrderByElement> orderbyRef = s.getOrderByElements();
		if (orderbyRef != null) {
			// pop out the top iter
			for (int i = 0; i < orderbyRef.size(); i++) {
				OrderByElement elem = orderbyRef.get(i);
				this.correct(orderbyRef.get(i).getExpression(), tables);
				//breaking selection operators with AND
				List<Expression> columns = Util.processSelect(orderbyRef.get(i).getExpression());
				for (int j = 0; j < columns.size(); j++) {
					Schema orderBySchema = Util.processExpression(columns.get(j));
					for (int k = 0; k < orderBySchema.getValues().size(); k++) {
						orderByColumns.add(new ExtendedColumn(orderBySchema.getValues().get(k)));
								
					}
				}
			}
		}
		
		// 4. check Having clause
		Expression having = s.getHaving();
		if (having != null) {
			// pop out the top iter
			this.correct(having, tables);
			//breaking selection operators with AND
			List<Expression> havings = Util.processSelect(having);

			for (int i = 0; i < havings.size(); i++) {
				Schema havingSchema = Util.processExpression(havings.get(i));
				for (int j = 0; j < havingSchema.getValues().size(); j++) {
					havingColumns.add(new ExtendedColumn(havingSchema.getValues().get(j)));
				}
			}
		}
		
		//5. check LIMIT clause
		Limit limit = s.getLimit();
		if(limit != null) {
			if (limitList.size() == 0)
				limitList.add("limit");
		}
		System.gc();
		
/*		if (queryOrder == 1) {
			projectionList.addAll((HashSet<Column>)projectionColumns.clone());
			selectionList.addAll((HashSet<Column>)selectionColumns.clone());
			groupbyList.addAll((HashSet<Column>)groupByColumns.clone());
			orderbyList.addAll((HashSet<Column>)orderByColumns.clone());
			havingList.addAll((HashSet<Column>)havingColumns.clone());
			joinPredicateList.addAll(joinPredicates);
		}
		*/ 
	}
	
	private void executeSelectWithAlias(SelectBody body, int queryOrder, String alias){
		if (body instanceof PlainSelect){
			((PlainSelect) body).getFromItem().setAlias(alias);
			executePlainSelect((PlainSelect)body, queryOrder);
		} else if (body instanceof Union) {
			List<PlainSelect> selectItems = ((Union)body).getPlainSelects();
			for (PlainSelect selectItem : selectItems) {
				executeSelectWithAlias((SelectBody)selectItem, queryOrder, alias);
			}
		}
	}
	
	//it returns the top iterator after executing the selectbody
	private void executeSelect(SelectBody body, int queryOrder){
		if (body instanceof PlainSelect){
			executePlainSelect((PlainSelect)body, queryOrder);
		} else if (body instanceof Union) {
			List<PlainSelect> selectItems = ((Union)body).getPlainSelects();
			for (PlainSelect selectItem : selectItems) {
				executePlainSelect(selectItem, queryOrder);
			}
		}
	}
		
	public void createQueryVector(Statement stmt1) {
		if (stmt1 instanceof Select) {
			Select s1=(Select) stmt1;
		//	System.out.println(s1.toString());
			List<WithItem> with1 = s1.getWithItemsList();
			if (with1 != null) {
				for (int i = 0; i < with1.size(); i++) {
					executeSelect(with1.get(i).getSelectBody(), 1);
				}
			}		
			//Collect where and group by and order by and having and aggregates
			executeSelect(s1.getSelectBody(), 1);
		}
		else if(stmt1 instanceof Update) {
			Update u1 = (Update) stmt1;
	//		System.out.println(u1.toString());
			ArrayList<Column> cols = (ArrayList<Column>) u1.getColumns();
			for(Column col:cols) {
				this.projectionColumns.add(col);
			}
			Table table = u1.getTable();
			consumeTable(table);
			ArrayList<Expression> otherExprs = (ArrayList<Expression>) u1.getExpressions();
			Expression whereExpr = u1.getWhere();
			parseWhere(whereExpr);
		}
		else if(stmt1 instanceof Insert) {
			Insert i1 = (Insert) stmt1;
	//		System.out.println(i1.toString());
			ArrayList<Column> cols = (ArrayList<Column>) i1.getColumns();
			for(Column col:cols) {
				this.projectionColumns.add(col);
			}
			Table table = i1.getTable();
			consumeTable(table);
		}
		else if(stmt1 instanceof Delete) {
			Delete d1 = (Delete) stmt1;
		//	System.out.println(d1);
			Table table = d1.getTable();
			consumeTable(table);
			Expression whereExpr = d1.getWhere();
			parseWhere(whereExpr);
		}
	/*	else {
			System.err.println(stmt1 + " is not a Select/Insert/Update/Delete query");
		}
		System.out.println("TABLES: "+ this.tables);
		System.out.println("TABLE ALIASES: "+ Global.tableAlias);
		System.out.println("GROUP BY: "+ this.groupByColumns);
		System.out.println("SELECT(WHERE): "+this.selectionColumns);
		System.out.println("PROJECT: "+this.projectionColumns);
		System.out.println("ORDER BY: "+this.orderByColumns);
		System.out.println("HAVING: "+this.havingColumns);
		System.out.println("JOIN: "+this.joinPredicates);
		System.out.println("LIMIT: "+this.limitList);
		System.out.println("MAX: "+this.MAXColumns);
		System.out.println("MIN: "+this.MINColumns);
		System.out.println("AVG: "+this.AVGColumns);
		System.out.println("SUM: "+this.SUMColumns);
		System.out.println("COUNT: "+this.COUNTColumns); */
	}
}


/**** OLD CODE ***
private void parseWhereOps(Expression where) {
if(this.dataset.equals("MINC"))
	parseMINCWhereOps(where);
else if(this.dataset.equals("BusTracker"))
	parseBusTrackerWhereOps(where);
else {
	System.out.println("Dataset does not Exist !!");
	System.exit(0);
}
}

private void parseBusTrackerWhereOps(Expression where) {
if (where != null) {
	// pop out the top iter
	this.correct(where, tables);
	//breaking selection operators with AND
	List<Expression> selects = Util.processSelect(where);
	assert selects.size()%2==0; // number of selects should be even because every where predicate is a binary expression
	// here we obtain join predicates by counting that even numbered values are constants
	// for instance, table.col1=table2.col2 will produce a column and a column, whereas table.col1=constant will produce a column and a constant
	ArrayList<Schema> selectJoinSchemas = new ArrayList<Schema>();
	for (int i = 0; i < selects.size(); i++) {
		if(selects.get(i).toString().contains(" ")) { // for exprs like "colName IS NULL", this turns IS NULL into a constant and adds it in between
			Schema selectJoinSchema = Util.processExpression(selects.get(i));
			selectJoinSchemas.add(selectJoinSchema);
			Schema constSchema = new Schema(); // for constant equivalent expression
			selectJoinSchemas.add(constSchema);
		} 
		else if(selects.get(i).toString().contains("\"") || selects.get(i).toString().contains("\'") || selects.get(i) instanceof LongValue || selects.get(i) instanceof StringValue 
				|| selects.get(i) instanceof DateValue || selects.get(i) instanceof TimestampValue || selects.get(i) instanceof DoubleValue || selects.get(i) instanceof TimeValue 
				|| selects.get(i) instanceof NullValue) { // not a column but a string constant, numerical constants and single parentheses are already covered
			Schema constSchema = new Schema(); // for constant equivalent expression
			selectJoinSchemas.add(constSchema);
		} 
		else {
			Schema selectJoinSchema = Util.processExpression(selects.get(i));
			selectJoinSchemas.add(selectJoinSchema);
		}
	}
	for (int schemaIndex=0; schemaIndex<selectJoinSchemas.size(); schemaIndex+=2) {
		//do checks at even junctures
		// check if both the entries at j and j+1 are columns
		if (selectJoinSchemas.get(schemaIndex).getValues().size() > 0 && selectJoinSchemas.get(schemaIndex+1).getValues().size() > 0) {
			ArrayList<Column> joinPredicate = new ArrayList<Column>();
			for(int tempIndex=schemaIndex; tempIndex<schemaIndex+2; tempIndex++) {
				for (int j = 0; j < selectJoinSchemas.get(tempIndex).getValues().size(); j++) {
					joinPredicate.add(new ExtendedColumn(selectJoinSchemas.get(tempIndex).getValues().get(j)));
				}
			}
			this.joinPredicates.add(joinPredicate);
		}
		else {
			for(int tempIndex=schemaIndex; tempIndex<schemaIndex+2; tempIndex++) {
				for (int j = 0; j < selectJoinSchemas.get(tempIndex).getValues().size(); j++) {
					this.selectionColumns.add(new ExtendedColumn(selectJoinSchemas.get(tempIndex).getValues().get(j)));
				}
			}
		}
	}
}
}

private void parseMINCWhereOps(Expression where) {
if (where != null) {
	// pop out the top iter
	this.correct(where, tables);
	//breaking selection operators with AND
	List<Expression> selects = Util.processSelect(where);
	assert selects.size()%2==0; // number of selects should be even because every where predicate is a binary expression
	// here we obtain join predicates by counting that even numbered values are constants
	// for instance, table.col1=table2.col2 will produce a column and a column, whereas table.col1=constant will produce a column and a constant
	ArrayList<Schema> selectJoinSchemas = new ArrayList<Schema>();
	for (int i = 0; i < selects.size(); i++) {
		if(selects.get(i).toString().contains(" ")) { // for exprs like "colName IS NULL", this turns IS NULL into a constant and adds it in between
			Schema selectJoinSchema = Util.processExpression(selects.get(i));
			selectJoinSchemas.add(selectJoinSchema);
			Schema constSchema = new Schema(); // for constant equivalent expression
			selectJoinSchemas.add(constSchema);
		} 
		else if(selects.get(i).toString().contains("\"")) { // not a column but a string constant, numerical constants and single parentheses are already covered
			Schema constSchema = new Schema(); // for constant equivalent expression
			selectJoinSchemas.add(constSchema);
		} 
		else {
			Schema selectJoinSchema = Util.processExpression(selects.get(i));
			selectJoinSchemas.add(selectJoinSchema);
		}
	}
	for (int schemaIndex=0; schemaIndex<selectJoinSchemas.size(); schemaIndex+=2) {
		//do checks at even junctures
		// check if both the entries at j and j+1 are columns
		if (selectJoinSchemas.get(schemaIndex).getValues().size() > 0 && selectJoinSchemas.get(schemaIndex+1).getValues().size() > 0) {
			ArrayList<Column> joinPredicate = new ArrayList<Column>();
			for(int tempIndex=schemaIndex; tempIndex<schemaIndex+2; tempIndex++) {
				for (int j = 0; j < selectJoinSchemas.get(tempIndex).getValues().size(); j++) {
					joinPredicate.add(new ExtendedColumn(selectJoinSchemas.get(tempIndex).getValues().get(j)));
				}
			}
			this.joinPredicates.add(joinPredicate);
		}
		else {
			for(int tempIndex=schemaIndex; tempIndex<schemaIndex+2; tempIndex++) {
				for (int j = 0; j < selectJoinSchemas.get(tempIndex).getValues().size(); j++) {
					this.selectionColumns.add(new ExtendedColumn(selectJoinSchemas.get(tempIndex).getValues().get(j)));
				}
			}
		}
	}
}
}

private void parseJoinListOps(List<Join> joinlist, int queryOrder) {
for (int i = 0; i < joinlist.size(); i++) {
	consumeFromItem(joinlist.get(i).getRightItem(), tables, queryOrder);
	
	Expression sss = joinlist.get(i).getOnExpression();
	//System.out.println(sss);
	if (sss != null) {
		// pop out the top iter
		this.correct(sss, tables);
		//breaking selection operators with AND
		List<Expression> joins = Util.processSelect(sss);
		ArrayList<Column> joinPredicate = null;
		for (int j = 0; j < joins.size(); j++) {
			if (j%2==0) {
				if(joinPredicate != null && joinPredicate.size() == 2)
					joinPredicates.add(joinPredicate);
				else if(joinPredicate != null && joinPredicate.size() == 1)
					selectionColumns.add(joinPredicate.get(0));
				joinPredicate = new ArrayList<Column>();
			}
			Schema joinSchema = Util.processExpression(joins.get(j));
			for (int k = 0; k < joinSchema.getValues().size(); k++) {							
				joinPredicate.add(new ExtendedColumn(joinSchema.getValues().get(k)));
			}
		}
		if(joinPredicate != null && joinPredicate.size() == 2)
			joinPredicates.add(joinPredicate);
		else if(joinPredicate != null && joinPredicate.size() == 1)
			selectionColumns.add(joinPredicate.get(0));
	}
	
	parseUsingColumns(joinlist, i);
}
}
******/