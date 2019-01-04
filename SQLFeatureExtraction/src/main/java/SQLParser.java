import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
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
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
//import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
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

public class SQLParser{
	String originalSQL;
	String rewrittenSQL;
	List<Table> tables = new ArrayList<Table>();
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
	
	public SQLParser(String originalSQL) {
		this.originalSQL = originalSQL;
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
		} else if (fromitem instanceof SubSelect){
			SubSelect temp = (SubSelect) fromitem;
			
			executeSelect(temp.getSelectBody(), queryOrder);
		}
	}
	
	private void parseGroupBy(List<Expression> groupbyRef) {
		if (groupbyRef != null) {
			// pop out the top iter
			for (int i = 0; i < groupbyRef.size(); i++) {
				SelectItemListParser.correct(groupbyRef.get(i), tables);
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
	
	private void parseWhere(Expression where) {
		if (where != null) {
			// pop out the top iter
			SelectItemListParser.correct(where, tables);
			//breaking selection operators with AND
			List<Expression> selects = Util.processSelect(where);
			assert selects.size()%2==0; // number of selects should be even because every where predicate is a binary expression
			// here we do a hack to obtain join predicates by counting that even numbered values are constants
			// for instance, table.col1=table2.col2 will produce a column and a column, whereas table.col1=constant will produce a column and a constant
			ArrayList<Schema> selectJoinSchemas = new ArrayList<Schema>();
			for (int i = 0; i < selects.size(); i++) {
				Schema selectJoinSchema = Util.processExpression(selects.get(i));
				selectJoinSchemas.add(selectJoinSchema);
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
				for (int i = 0; i < joinlist.size(); i++) {
					consumeFromItem(joinlist.get(i).getRightItem(), tables, queryOrder);
					
					Expression sss = joinlist.get(i).getOnExpression();
					//System.out.println(sss);
					if (sss != null) {
						// pop out the top iter
						SelectItemListParser.correct(sss, tables);
						//breaking selection operators with AND
						List<Expression> joins = Util.processSelect(sss);
						ArrayList<Column> joinPredicate = null;
						for (int j = 0; j < joins.size(); j++) {
							if (j%2==0) {
								if(joinPredicate != null)
									joinPredicates.add(joinPredicate);
								joinPredicate = new ArrayList<Column>();
							}
							Schema joinSchema = Util.processExpression(joins.get(j));
							for (int k = 0; k < joinSchema.getValues().size(); k++) {							
								joinPredicate.add(new ExtendedColumn(joinSchema.getValues().get(k)));
							}
						}
						if(joinPredicate!=null)
							joinPredicates.add(joinPredicate);
					}
					
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
				}
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
					if (sss instanceof SubSelect) {
						SubSelect temp = (SubSelect) sss;
						
						executeSelect(temp.getSelectBody(), queryOrder);
					}
					Schema selectSchema = Util.processExpression(sss);
					if (sss instanceof Function){
						Function f=(Function)sss;  		
						String fName=f.getName();
						if (fName.equals("MAX")){
							for (int j = 0; j < selectSchema.getValues().size(); j++) {
								MAXColumns.add(new ExtendedColumn(selectSchema.getValues().get(j)));
							}
						}
						else if(fName.equals("MIN")){
							for (int j = 0; j < selectSchema.getValues().size(); j++) {
								MINColumns.add(new ExtendedColumn(selectSchema.getValues().get(j)));
							}
						}
						else if (fName.equals("SUM")){
							for (int j = 0; j < selectSchema.getValues().size(); j++) {
								SUMColumns.add(new ExtendedColumn(selectSchema.getValues().get(j)));
							}
						}
						else if (fName.equals("AVG")){
							for (int j = 0; j < selectSchema.getValues().size(); j++) {
								AVGColumns.add(new ExtendedColumn(selectSchema.getValues().get(j)));
							}
						}
						else if (fName.equals("COUNT")){
							for (int j = 0; j < selectSchema.getValues().size(); j++) {
								COUNTColumns.add(new ExtendedColumn(selectSchema.getValues().get(j)));
							}
						}
					}
					for (int j = 0; j < selectSchema.getValues().size(); j++) {
						projectionColumns.add(new ExtendedColumn(selectSchema.getValues().get(j)));
					}
				}
				else if (ss instanceof SubSelect) {
					SubSelect temp = (SubSelect) ss;
					
					executeSelect(temp.getSelectBody(), queryOrder);
				}
				else if (ss instanceof AllColumns){
					projectionColumns.add(new ExtendedColumn("*"));
				}
				else if (ss instanceof AllTableColumns) {
					projectionColumns.add(new ExtendedColumn(ss.toString()));
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
				SelectItemListParser.correct(orderbyRef.get(i).getExpression(), tables);
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
			SelectItemListParser.correct(having, tables);
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
				limitList.add("LIMIT");
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
	
	//it returns the top iterator after executing the selectbody
	private void executeSelect(SelectBody body, int queryOrder){
		if (body instanceof PlainSelect){
			executePlainSelect((PlainSelect)body, queryOrder);
		}
	}
		
	public void createQueryVector(Statement stmt1) {
		if (stmt1 instanceof Select) {
			Select s1=(Select) stmt1;
			System.out.println(s1.toString());
			List<WithItem> with1 = s1.getWithItemsList();
			if (with1 != null) {
				for (int i = 0; i < with1.size(); i++) {
					executeSelect(with1.get(i).getSelectBody(), 1);
				}
			}		
			//Collect where and group by and nothing else
			executeSelect(s1.getSelectBody(), 1);
		}
		else if(stmt1 instanceof Update) {
			Update u1 = (Update) stmt1;
			System.out.println(u1.toString());
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
			System.out.println(i1.toString());
			ArrayList<Column> cols = (ArrayList<Column>) i1.getColumns();
			for(Column col:cols) {
				this.projectionColumns.add(col);
			}
			Table table = i1.getTable();
			consumeTable(table);
		}
		else if(stmt1 instanceof Delete) {
			Delete d1 = (Delete) stmt1;
			System.out.println(d1);
			Table table = d1.getTable();
			consumeTable(table);
			Expression whereExpr = d1.getWhere();
			parseWhere(whereExpr);
		}
		else {
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
		System.out.println("COUNT: "+this.COUNTColumns);
	}
}