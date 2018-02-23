/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.ppal.lib.db;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.log4j.Logger;

/**
 *
 * @author kumasati
 */
// TODO: LOG messages
public class DBCallableOperations {

	private  final Logger DBLogger = Logger.getLogger(DbConnectionClose.class.getName());
	private static DBConnection DBCon;

	public DBCallableOperations(String PropFileName) {

		DBCon = new DBConnection(PropFileName);

	}

	// Execute the insert operation
	public boolean executeCallableInsert(String query, String[] params, String[] paramType) {

		Connection con = DBCon.getConnection();
		try {
			CallableStatement cstmt = con.prepareCall(query);

			for (int i = 1; i <= params.length; i++) {

				switch (paramType[i - 1]) {
				case "String":
					cstmt.setString(i, params[i - 1]);
					break;
				case "int":
					cstmt.setInt(i, Integer.parseInt(params[i - 1]));
					break;
				case "long":
					cstmt.setLong(i, Long.parseLong(params[i - 1]));
					break;
				case "double":
					cstmt.setDouble(i, Double.parseDouble(params[i - 1]));
					break;
				default:
					cstmt.setString(i, params[i - 1]);
					break;

				}

			}

			int insertResult = cstmt.executeUpdate();
			DBCon.commitOperation(con);
			DbConnectionClose.closeConnection(con);
			DbConnectionClose.closeCallableStatement(cstmt);
			DBLogger.info("Insert Operation sucessful");
			return insertResult > 0;
		} catch (SQLException ex) {

			DBCon.RollbackOperation(con);
			DBLogger.error(ex.getLocalizedMessage());
			return false;
		}

	}

	// execute update operation
	public boolean executeCallableUpdate(String query, String[] params, String paramType[]) {
		Connection con = DBCon.getConnection();
		try {
			CallableStatement cstmt = con.prepareCall(query);

			for (int i = 1; i <= params.length; i++) {

				switch (paramType[i - 1]) {
				case "String":
					cstmt.setString(i, params[i - 1]);
					break;
				case "int":
					cstmt.setInt(i, Integer.parseInt(params[i - 1]));
					break;
				case "long":
					cstmt.setLong(i, Long.parseLong(params[i - 1]));
					break;
				case "double":
					cstmt.setDouble(i, Double.parseDouble(params[i - 1]));
					break;
				default:
					cstmt.setString(i, params[i - 1]);
					break;

				}

			}

			int updateResult = cstmt.executeUpdate();
			DBCon.commitOperation(con);
			DbConnectionClose.closeConnection(con);
			DbConnectionClose.closeCallableStatement(cstmt);
			DBLogger.info("Update Operation sucessful");
			return updateResult > 0;
		} catch (SQLException ex) {
			DBLogger.error(ex.getLocalizedMessage());
			DBCon.RollbackOperation(con);
			return false;
		}

	}

	// Execute select operation FOR sTRING
	public String executeCallableSelectString(String query, String[] inParams, String inParamType[], int outIndex) {
		Connection con = DBCon.getConnection();
		try {
			CallableStatement cstmt = con.prepareCall(query);
			cstmt.registerOutParameter(outIndex, Types.VARCHAR);		
			for (int i = 1; i <= inParams.length; i++) {

				switch (inParamType[i - 1]) {
				case "String":
					cstmt.setString(i, inParams[i - 1]);
					break;
				case "int":
					cstmt.setInt(i, Integer.parseInt(inParams[i - 1]));
					break;
				case "long":
					cstmt.setLong(i, Long.parseLong(inParams[i - 1]));
					break;
				case "double":
					cstmt.setDouble(i, Double.parseDouble(inParams[i - 1]));
					break;
				default:
					cstmt.setString(i, inParams[i - 1]);
					break;

				}
				// cstmt.setString(i,params[i-1]);
			}
			//boolean hasResults = cstmt.execute();
			cstmt.execute();
			String REScstmt = null;
			//if(hasResults) {
				REScstmt = String.valueOf(cstmt.getInt(outIndex));
			//}
			DBCon.commitOperation(con);
			DbConnectionClose.closeConnection(con);
			DbConnectionClose.closeCallableStatement(cstmt);
			DBLogger.info("Select Operation sucessful");
			return REScstmt;
		} catch (SQLException ex) {
			DBLogger.error(ex.getLocalizedMessage());
			DBCon.RollbackOperation(con);
			return null;
		}

	}
	// Execute select operation FOR LONG
	public boolean executeCallableSelectBoolean(String query, String[] inParams, String inParamType[], int outIndex) {
		Connection con = DBCon.getConnection();
		try {
			CallableStatement cstmt = con.prepareCall(query);
			cstmt.registerOutParameter(outIndex, Types.BOOLEAN);		
			for (int i = 1; i <= inParams.length; i++) {

				switch (inParamType[i - 1]) {
				case "String":
					cstmt.setString(i, inParams[i - 1]);
					break;
				case "int":
					cstmt.setInt(i, Integer.parseInt(inParams[i - 1]));
					break;
				case "long":
					cstmt.setLong(i, Long.parseLong(inParams[i - 1]));
					break;
				case "double":
					cstmt.setDouble(i, Double.parseDouble(inParams[i - 1]));
					break;
				default:
					cstmt.setString(i, inParams[i - 1]);
					break;

				}
				// cstmt.setString(i,params[i-1]);
			}
			//boolean hasResults = cstmt.execute();
			cstmt.execute();
				//REScstmt = cstmt.getLong( outIndex);
			//}
			DBCon.commitOperation(con);
			DbConnectionClose.closeConnection(con);
			DbConnectionClose.closeCallableStatement(cstmt);
			DBLogger.info("Select Operation sucessful");
			return true;
		} catch (SQLException ex) {
			DBLogger.error(ex.getLocalizedMessage());
			DBCon.RollbackOperation(con);
			return false;
		}

	}
	// Execute select operation FOR DOUBLE
	public double executeCallableSelectDouble(String query, String[] inParams, String inParamType[], int outIndex) {
		Connection con = DBCon.getConnection();
		try {
			CallableStatement cstmt = con.prepareCall(query);
			cstmt.registerOutParameter(outIndex, Types.DOUBLE);		
			for (int i = 1; i <= inParams.length; i++) {

				switch (inParamType[i - 1]) {
				case "String":
					cstmt.setString(i, inParams[i - 1]);
					break;
				case "int":
					cstmt.setInt(i, Integer.parseInt(inParams[i - 1]));
					break;
				case "long":
					cstmt.setLong(i, Long.parseLong(inParams[i - 1]));
					break;
				case "double":
					cstmt.setDouble(i, Double.parseDouble(inParams[i - 1]));
					break;
				default:
					cstmt.setString(i, inParams[i - 1]);
					break;

				}
				// cstmt.setString(i,params[i-1]);
			}
			//boolean hasResults = cstmt.execute();
			cstmt.execute();
			int REScstmt = -1;
			//if(hasResults) {
				REScstmt = cstmt.getInt(outIndex);
			//}
			DBCon.commitOperation(con);
			DbConnectionClose.closeConnection(con);
			DbConnectionClose.closeCallableStatement(cstmt);
			DBLogger.info("Select Operation sucessful");
			return REScstmt;
		} catch (SQLException ex) {
			DBLogger.error(ex.getLocalizedMessage());
			DBCon.RollbackOperation(con);
			return -1;
		}

	}
	// Execute select operation FOR INTEGER
	public int executeCallableSelectInteger(String query, String[] inParams, String inParamType[], int outIndex) {
		Connection con = DBCon.getConnection();
		try {
			CallableStatement cstmt = con.prepareCall(query);
			cstmt.registerOutParameter(outIndex, Types.INTEGER);		
			for (int i = 1; i <= inParams.length; i++) {

				switch (inParamType[i - 1]) {
				case "String":
					cstmt.setString(i, inParams[i - 1]);
					break;
				case "int":
					cstmt.setInt(i, Integer.parseInt(inParams[i - 1]));
					break;
				case "long":
					cstmt.setLong(i, Long.parseLong(inParams[i - 1]));
					break;
				case "double":
					cstmt.setDouble(i, Double.parseDouble(inParams[i - 1]));
					break;
				default:
					cstmt.setString(i, inParams[i - 1]);
					break;

				}
				// cstmt.setString(i,params[i-1]);
			}
			//boolean hasResults = cstmt.execute();
			cstmt.execute();
			int REScstmt = -1;
			//if(hasResults) {
				REScstmt = cstmt.getInt(outIndex);
			//}
			DBCon.commitOperation(con);
			DbConnectionClose.closeConnection(con);
			DbConnectionClose.closeCallableStatement(cstmt);
			DBLogger.info("Select Operation sucessful");
			return REScstmt;
		} catch (SQLException ex) {
			DBLogger.error(ex.getLocalizedMessage());
			DBCon.RollbackOperation(con);
			return -1;
		}

	}
	
	// Execute select operation FOR LONG
		public long executeCallableSelect(String query, String[] inParams, String inParamType[], int outIndex) {
			Connection con = DBCon.getConnection();
			try {
				CallableStatement cstmt = con.prepareCall(query);
				cstmt.registerOutParameter((int) outIndex, Types.BIGINT);		
				for (int i = 1; i <= inParams.length; i++) {

					switch (inParamType[i - 1]) {
					case "String":
						cstmt.setString(i, inParams[i - 1]);
						break;
					case "int":
						cstmt.setInt(i, Integer.parseInt(inParams[i - 1]));
						break;
					case "long":
						cstmt.setLong(i, Long.parseLong(inParams[i - 1]));
						break;
					case "double":
						cstmt.setDouble(i, Double.parseDouble(inParams[i - 1]));
						break;
					default:
						cstmt.setString(i, inParams[i - 1]);
						break;

					}
					// cstmt.setString(i,params[i-1]);
				}
				//boolean hasResults = cstmt.execute();
				cstmt.execute();
				long REScstmt = -1;
				//if(hasResults) {
					REScstmt = cstmt.getLong( outIndex);
				//}
				DBCon.commitOperation(con);
				DbConnectionClose.closeConnection(con);
				DbConnectionClose.closeCallableStatement(cstmt);
				DBLogger.info("Select Operation sucessful");
				return REScstmt;
			} catch (SQLException ex) {
				DBLogger.error(ex.getLocalizedMessage());
				DBCon.RollbackOperation(con);
				return -1;
			}

		}

	// Execute delete operation
	public boolean executeCallableDelete(String query, String[] params, String paramType[]) {
		Connection con = DBCon.getConnection();
		try {
			CallableStatement cstmt = con.prepareCall(query);

			for (int i = 1; i <= params.length; i++) {

				switch (paramType[i - 1]) {
				case "String":
					cstmt.setString(i, params[i - 1]);
					break;
				case "int":
					cstmt.setInt(i, Integer.parseInt(params[i - 1]));
					break;
				case "long":
					cstmt.setLong(i, Long.parseLong(params[i - 1]));
					break;
				case "double":
					cstmt.setDouble(i, Double.parseDouble(params[i - 1]));
					break;
				default:
					cstmt.setString(i, params[i - 1]);
					break;

				}

			}

			int deleteResult = cstmt.executeUpdate();
			DBCon.commitOperation(con);
			DbConnectionClose.closeConnection(con);
			DbConnectionClose.closeCallableStatement(cstmt);
			DBLogger.info("Delete Operation sucessful");
			return deleteResult > 0;
		} catch (SQLException ex) {
			DBLogger.error(ex.getLocalizedMessage());
			DBCon.RollbackOperation(con);
			return false;
		}

	}
	// Execute select operation
		public ResultSet executeCallablerReturn(String query, String[] inParams, String inParamType[], int outIndex) {
			Connection con = DBCon.getConnection();
			ResultSet rs = null;    
			try {
				CallableStatement cstmt = con.prepareCall(query);
				cstmt.registerOutParameter(outIndex, Types.VARCHAR);		
				for (int i = 1; i <= inParams.length; i++) {

					switch (inParamType[i - 1]) {
					case "String":
						cstmt.setString(i, inParams[i - 1]);
						break;
					case "int":
						cstmt.setInt(i, Integer.parseInt(inParams[i - 1]));
						break;
					case "long":
						cstmt.setLong(i, Long.parseLong(inParams[i - 1]));
						break;
					case "double":
						cstmt.setDouble(i, Double.parseDouble(inParams[i - 1]));
						break;
					default:
						cstmt.setString(i, inParams[i - 1]);
						break;

					}
					
				}
				
				String REScstmt = null;
				rs=cstmt.executeQuery();
				if(rs.next()) {
					REScstmt = rs.getString(outIndex);
				}
				DBCon.commitOperation(con);
				DbConnectionClose.closeConnection(con);
				DbConnectionClose.closeCallableStatement(cstmt);
				DBLogger.info("Select Operation sucessful");
				return rs;
			} catch (SQLException ex) {
				DBLogger.error(ex.getLocalizedMessage());
				DBCon.RollbackOperation(con);
				
				System.out.println("here");
				return null;
			}

		}
		

}
