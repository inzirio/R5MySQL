####################################################################################
## Copyryght (C) 2014 Dario Righelli                                              ##
##                                                                                ##
## R5MySQL is free software: you can redistribute it and/or modify                ##
## it under the terms of the GNU General Public License as published by           ##
## the Free Software Foundation, either version 3 of the License, or              ##
## (at your option) any later version.                                            ##
##                                                                                ##
## This program is distributed in the hope that it will be useful,                ##
## but WITHOUT ANY WARRANTY; without even the implied warranty of                 ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the                  ##
## GNU General Public License for more details.                                   ##
##                                                                                ##
## You should have received a copy of the GNU General Public License              ##
## along with this program.  If not, see <http://www.gnu.org/licenses/>.          ##
####################################################################################
##  Database_R5_class: a R5 class to interface with RMySQL package                ##
##  Author e.mail: dario.righelli@gmail.com                                       ##
##  project site: https://github.com/inzirio/R5MySQL.git                          ##
##  actual version: 0.01v                                                         ##
##                                                                                ##
##  note: the author reserves the rights to change this class in any moment,      ##
##        Feel free to edit and improve this class, but please, to better         ##
##        coordinate, send your improvements or ideas to the author.              ##
##                                                                                ##
####################################################################################

if(!is.installed('RMySQL')) {
  install.packages('RMySQL');
}

library('RMySQL');

Database <- setRefClass( "Database_R5",
  fields = list (
    my.credentials = "list",
    my.name = "character",
    my.self = "MySQLConnection"
  ),
  
  methods = list (
    CreateDatabase = function() {
      my.self <<- dbConnect(MySQL(), user=my.credentials$user, password=my.credentials$password, 
                            port=my.credentials$port, host=my.credentials$host)
      a <- dbSendQuery(my.self, paste0('CREATE DATABASE IF NOT EXISTS', my.name,'CHARACTER SET=utf8;'))
      Disconnect()
    },
    Connect = function () {
      my.self <<- dbConnect(MySQL(), user=my.credentials$user, password=my.credentials$password, 
                            port=my.credentials$port, host=my.credentials$host, dbname=my.name)
    },
    Disconnect = function () {
      dbDisconnect(my.self)
    },
    CreateTable = function (create.table.query) {
      #query <- paste0
      a <- dbSendQuery(my.self, create.table.query)
      dbClearResult(a)
    },
    RenameTable=function(table.from.name, table.to.name) {
      query<-paste('RENAME TABLE', table.from.name,'TO', table.to.name,';', sep=' ')
      a <- dbSendQuery(my.self, query)
      dbClearResult(a)
        
    },
    PopulateTable = function(table.name, population.dataframe, p.append=T, p.rownames=F, p.overwrite=F) {
      dbWriteTable(my.self, name=table.name, value=population.dataframe, append=p.append, row.names=p.rownames, overwrite=p.overwrite);
    },
    GetQueryResults = function(interrogation.query) {
      a <- dbSendQuery(my.self, interrogation.query)
      out <- fetch(a, n=100000000)#-1)
      dbClearResult(a)
      return(out)
    },
    GetQueryResults = function(columns, table.name, where.clause=NULL) {
      interrogation.query <- paste0('SELECT', columns, ' FROM ', table.name)
      if(where.clause!=NULL) {
        interrogation.query <- paste0(interrogation.query, ' WHERE ', where.clause)
      }
      a <- dbSendQuery(my.self, interrogation.query)
      out <- fetch(a, n=100000000)
      dbClearResult(a)
      return(out)
    }
  )
)