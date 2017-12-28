"""
	Autor : Payel Das
	Description : PyQt4 custom Hive table creator.
	Created : March 10, 2017
	
"""

import sys
reload(sys)
sys.setdefaultencoding('utf8')
from PyQt4.QtGui import *
from PyQt4 import QtCore, QtGui
import pyodbc
import getpass
import time
import functools


class SplitGene(QtGui.QMainWindow):
    """ Class definition for the custom Hive table that can select datasouce and tables
    	options and create the H table screate statments .
    """
    def __init__(self, parent=None):
        super(SplitGene, self).__init__(parent)
        self.win_widget = SplitterMain(self)
        widget = QtGui.QWidget()
        layout = QtGui.QVBoxLayout(widget)
        layout.addWidget(self.win_widget)
        self.setCentralWidget(widget)
        menubar = self.menuBar()
        extractAction = QtGui.QAction("&Leave the App!!!", self)
        extractAction.setShortcut("Ctrl+Q")
        extractAction.triggered.connect(self.win_widget.close_application)
        extractAction1 = QtGui.QAction("&Help", self)
        extractAction1.setShortcut("Ctrl+H")
        extractAction1.triggered.connect(functools.partial(self.table))
        fileMenu = menubar.addMenu('&Menu')
        fileMenu.addAction(extractAction)
        fileMenu.addAction(extractAction1)
        self.setGeometry(800, 600, 800, 600)
        self.setWindowTitle('Hive Table Generator Beta')
        self.setWindowIcon(QtGui.QIcon('icon.ico'))
        self.center()
        self.show()

    def center(self):
        qr = self.frameGeometry()
        cp = QtGui.QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())

    def table(self):
        stmt1 = 'You need to provide the userid and password '
        stmt2 = 'Then you need to select one of the data source listed .'
        stmt3 = 'Please make sure you have all the ODBC connetion already setup up.'
        stmt4 = 'For the Hadoop/ Postgre SQL select the appropiate box .'
        stmt5 = 'You have the options to select all the table in the selected Datasource'
        stmt6 = 'OR you can select the indivual table.'
        stmt7 = 'Creating Hive tables is really an easy task.'
        stmt8  = 'Get the list of tables from a RDBMS database'
        stmt9 =  'generate Hive Scripts with automatic data type mapping'

        sqlstat2 = 'Hive Table Statment Created saved to %s \n %s \n %s \n %s \n %s \n %s ' \
                   '\n %s \n%s \n%s \n ' % (stmt1, stmt2 ,stmt3 ,stmt4 ,stmt5, stmt6, stmt7, stmt8 ,stmt9)
        QMessageBox.about(self, self.tr("Message"), self.tr("\n%2").arg(sqlstat2))


class SplitterMain(QtGui.QWidget):
    def __init__(self, parent):
        super(SplitterMain,self).__init__(parent)


        self.initUI()

    def initUI(self):
        """ Initialization for Hivetable creator class.

        	Args:
        		parent (QWidget): parent widget to which the Htable creator is attached.
        """
        self.hbox = QtGui.QHBoxLayout(self)

        self.fs2= ''
        self.fs3= ''
        self.lcheckvalue = 'NOHADOOP'
        self.rcheckvalue = 'NO'
        self.sourcetabnam = ''
        self.targetabnam  = ''
        self.sourc1col = ''
        self.sourc1co2 = ''





        self.topleft = QtGui.QFrame(self)
        self.topleft.setFrameShape(QtGui.QFrame.StyledPanel)
        self.topleft.setFixedSize(420,60)



        sources = pyodbc.dataSources()
        foods= sources.keys()
        foods.sort()
        self.topleft.salutations = foods
        self.topleft.salutation = QComboBox(self.topleft)
        self.topleft.salutation.addItem("Select Source Datasource")

        for i in range(len(foods)):
            self.topleft.salutation.addItem(foods[i])

        self.topleft.salutation.move(20, 20)

        self.topleft.hadcheck = QtGui.QCheckBox('IsHadoop/Greenplum', self.topleft)
        self.topleft.hadcheck.move(190, 20)
        self.topleft.hadcheck.stateChanged.connect(self.leftchangeTitle)
        self.topleft.salutation.activated[str].connect(self.sourcetable)


        self.topright = QtGui.QFrame(self)
        self.topright.setFrameShape(QtGui.QFrame.StyledPanel)
        self.topright.setFixedSize(420,60)



        self.bottom = QtGui.QFrame(self)
        self.bottom.setFrameShape(QtGui.QFrame.StyledPanel)
        self.bottom.setGeometry(1210, 0, 1280, 0)

        self.bottom.printxt = QtGui.QLabel(self.bottom)

        self.bottom.printxt.setText("                                                ")
        self.bottom.printxt.setStyleSheet('font-family: Courier New;font-style: normal;font-size: 8pt;font-weight: bold;color: blue')
        self.bottom.printxt.move(28,20)
        self.bottom.printxt2 = QtGui.QLabel(self.bottom)
        self.bottom.printxt2.setText("                                               ")
        self.bottom.printxt2.setStyleSheet('font-family: Courier New;font-style: normal;font-size: 8pt;font-weight: bold;color: blue')
        self.bottom.printxt2.move(28,40)
        self.bottom.printxt3 = QtGui.QLabel(self.bottom)
        self.bottom.printxt3.setText("                                               ")
        self.bottom.printxt3.setStyleSheet('font-family: Courier New;font-style: normal;font-size: 8pt;font-weight: bold;color: blue')
        self.bottom.printxt3.move(28,60)
        self.bottom.printxt4 = QtGui.QLabel(self.bottom)
        self.bottom.printxt4.setText("                                               ")
        self.bottom.printxt4.setStyleSheet('font-family: Courier New;font-style: normal;font-size: 8pt;font-weight: bold;color: blue')
        self.bottom.printxt4.move(28,80)


        self.topleft1 = QtGui.QFrame(self)
        self.topleft1.setFrameShape(QtGui.QFrame.StyledPanel)
        self.topleft1.setFixedSize(420,60)

        self.topleft1.delsalutation = QComboBox(self.topleft1)
        self.topleft1.delsalutation.addItem("Select Source Tablename")
        self.topleft1.delsalutation.move(21, 20)

        self.topleft1.hadcheck = QtGui.QCheckBox('SelectAllTable', self.topleft1)
        self.topleft1.hadcheck.move(190, 20)
        self.topleft1.hadcheck.stateChanged.connect(self.rightchangeTitle)

        self.topleft1.delsalutation.activated[str].connect(self.sourcecol)


        self.topright1 = QtGui.QFrame(self)
        self.topright1.setFrameShape(QtGui.QFrame.StyledPanel)
        self.topright1.setFixedSize(420,60)

        self.topright12 = QtGui.QFrame(self)
        self.topright12.setFrameShape(QtGui.QFrame.StyledPanel)
        self.topright12.setFixedSize(420,60)

        self.scrollLayout2 = QtGui.QFormLayout()
        self.scrollLayout2.username = QtGui.QLineEdit()
        self.scrollLayout2.username.setPlaceholderText(getpass.getuser())
        self.scrollLayout2.password = QtGui.QLineEdit()
        self.scrollLayout2.password.setEchoMode(QtGui.QLineEdit.Password)
        self.scrollLayout2.username.setFixedSize(80,20)
        self.scrollLayout2.password.setFixedSize(80,20)
        self.scrollLayout2.addRow("Username", self.scrollLayout2.username)

        self.scrollLayout2.addRow("Password", self.scrollLayout2.password)
        self.scrollLayout2.setFormAlignment(QtCore.Qt.AlignRight)

        self.scrollWidget2 = QtGui.QWidget()
        self.scrollWidget2.setLayout(self.scrollLayout2)


        self.scrollArea2 = QtGui.QScrollArea()
        self.scrollArea2.setWidgetResizable(True)
        self.scrollArea2.setFixedSize(848,120)
        self.scrollArea2.setWidget(self.scrollWidget2)

        self.be4bottom12 = QtGui.QFrame(self)
        self.be4bottom12.setFrameShape(QtGui.QFrame.StyledPanel)
        self.be4bottom12.setFixedSize(850,40)
        self.be4bottom12.pushButton2 = QtGui.QPushButton('Proceed to HiveCreation', self.be4bottom12)
        self.be4bottom12.pushButton2.setFixedWidth(200)
        self.be4bottom12.pushButton2.move(300, 10)
        self.be4bottom12.pushButton2.clicked.connect(lambda:self.recognize())

        self.be4bottom12.progress = QtGui.QProgressBar(self.be4bottom12)

        self.be4bottom12.progress.setGeometry(100, 80, 250, 20)
        self.be4bottom12.progress.move(600,10)


        self.splitter1 = QtGui.QSplitter(QtCore.Qt.Horizontal)
        self.splitter1.addWidget(self.topleft)

        self.splitter1.addWidget(self.topright)

        self.splitter11 = QtGui.QSplitter(QtCore.Qt.Horizontal)
        self.splitter11.addWidget(self.topleft1)
        self.splitter11.addWidget(self.topright1)

        self.splitter12 = QtGui.QSplitter(QtCore.Qt.Horizontal)
        self.splitter12.addWidget(self.topright12)

        self.splitter2 = QtGui.QSplitter(QtCore.Qt.Vertical)
        self.splitter2.addWidget(self.scrollArea2)
        self.splitter2.addWidget(self.splitter1)
        self.splitter2.addWidget(self.splitter11)
        self.splitter2.addWidget(self.splitter12)
        self.splitter2.addWidget(self.be4bottom12)
        self.splitter2.addWidget(self.bottom)

        self.hbox.addWidget(self.splitter2)

        self.setLayout(self.hbox)
        QtGui.QApplication.setStyle(QtGui.QStyleFactory.create('Cleanlooks'))

        self.setGeometry(800, 600, 800, 600)
        self.setWindowTitle('NEW COMPARATOR beta')
        self.setWindowIcon(QtGui.QIcon('icon.ico'))
        self.center()
        self.show()

    def center(self):
        qr = self.frameGeometry()
        cp = QtGui.QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())

    def sourcetable(self, text):
        """ Function to obtain the list of avaliable datasouces for which user can generate statments.

        	Returns:
        		(str) list of datasouces (hadoop as well as no hadoop).
        """
        fs2 = text

        if len(str(self.scrollLayout2.username.text())) == 0:
            fs0 = getpass.getuser()
        else:
            fs0 = str(self.scrollLayout2.username.text())

        if len(str(self.scrollLayout2.password.text())) == 0:
            sqlstatx = 'Password cannot be blank'
            QMessageBox.about(self, self.tr("Warning"), self.tr("\n%2").arg(sqlstatx))
            fs1 = ' '
        else:
            fs1 = str(self.scrollLayout2.password.text())

        myerror = 0
        try:
            if self.lcheckvalue == 'NOHADOOP':
                conns = pyodbc.connect('DSN=%s;UID=%s;PWD=%s' % (fs2, fs0, fs1))
            else:
                conns = pyodbc.connect('DSN=%s;UID=%s;PWD=%s' % (fs2, fs0, fs1), autocommit=True, ansi=True,
                                       trusted_connection='yes')
        except pyodbc.Error as err:
            sqlstat0 = err.args[0]
            sqlstat1 = err.args[1]
            myerror = 1
            QMessageBox.about(self, self.tr("Warning"), self.tr("\n%2").arg(sqlstat1))

        if myerror == 0:
            cursor1 = conns.cursor()
            hpprint = []
            for row in cursor1.tables(tableType='TABLE'):
                hpprint.append('%s.%s' % (row.table_schem, row.table_name))
            self.topleft1.delsalutations = hpprint
            for i in range(len(hpprint)):
                self.topleft1.delsalutation.addItem(hpprint[i])
            self.topleft1.delsalutation.move(20, 10)
            self.fs2 = fs2
            self.fs0 = fs0
            self.fs1 = fs1
            self.hpprint = hpprint

            return self.fs2, self.fs0, self.fs1, self.hpprint



    def sourcecol(self,text):
        """ Function to obtain the selected table by the user.

        	Returns:
        		(str) return the selected tablename.
        """
        fs2 = self.fs2
        self.sourcetabnam = text
        return self.sourcetabnam


    def leftchangeTitle(self, state):
        """ Function to validate if Hadoop data source is selected.

        	Returns:
        		(str) return if Hadoop or NoHaoop is selected.
        """
        if state == QtCore.Qt.Checked:
            self.lcheckvalue = 'HADOOP'
        else:
            self.lcheckvalue = 'NOHADOOP'
        return self.lcheckvalue

    def rightchangeTitle(self, state):
        """ Function to validate if All table is selected or specifc ones.

        	Returns:
        		(str) return the if ALL or NO is selected.
        """
        if state == QtCore.Qt.Checked:
            self.rcheckvalue = 'ALL'
        else:
            self.rcheckvalue = 'NO'
        return self.rcheckvalue




    def recognize(self):
        """ Function to creates the statments for the Hive table as well as insert stamtents for the selected table.

        	Returns:
        		Save the created statments for all the tables or the selected table for the selected datasource in a file.
        """
        self.completed = 0
        fs0 = self.fs0
        fs1 = self.fs1
        scmyerror = 0



        if  self.fs2 != '':
                if self.rcheckvalue == 'ALL':

                    tableslist = time.strftime("%Y%m%d%H%M%S") + 'Tablelist.txt'

                    thefile = open(tableslist, 'w')
                    for item in self.hpprint:
                        thefile.write("%s\n" % item)
                    stmt1 = 'The number of tables getting process is :'

                    stmt2 = str((len(self.hpprint)))
                    sqlstat2 = ' %s \n %s \n ' % (stmt1, stmt2)
                    QMessageBox.about(self, self.tr("Message"), self.tr("\n%2").arg(sqlstat2))
                    thefile.close()
                else:

                    tableslist = time.strftime("%Y%m%d%H%M%S") + 'Tablelist.txt'

                    thefile = open(tableslist, 'w')
                    thefile.write("%s\n" % self.sourcetabnam)

                    thefile.close()



                createHiveStmt = time.strftime("%Y%m%d%H%M%S") + 'createHiveORCStmt.txt'
                createHiveingestion = time.strftime("%Y%m%d%H%M%S") + 'createHiveingestionStmt.txt'

                input_file = open(tableslist, 'r')
                createHiveStmt_file = open(createHiveStmt, 'w')
                Hiveingestion_file = open(createHiveingestion, 'w')
                getColValTypes = []
                getColNames = []
                hiveAVROSchema = "AVRODB."
                hiveORCSchema = "ORCDB."
                tablecreation = ["CREATE EXTERNAL TABLE",
                                 " PARTITIONED BY (load_year INT,load_month INT,load_date INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ",
                                 " STORED AS ORC;"]
                ingestionStmt = [
                    "SET tez.queue.name=default;SET hive.exec.dynamic.partition = true;SET hive.exec.dynamic.partition.mode=nonstrict;SET hive.execution.engine=tez;SET mapreduce.framework.name=yarn-tez;SET hive.exec.max.dynamic.partitions=100000;SET hive.exec.max.dynamic.partitions.pernode=100000;",
                    "INSERT TABLE ", "PARTITION (LOAD_YEAR,LOAD_MONTH,LOAD_DATE) ",
                    ",LOAD_YEAR,LOAD_MONTH,LOAD_DATE from "]

                try:
                    if self.lcheckvalue == 'NOHADOOP':
                        conn = pyodbc.connect('DSN=%s;UID=%s;PWD=%s' % (self.fs2, fs0, fs1))
                    else:
                        conn = pyodbc.connect('DSN=%s;UID=%s;PWD=%s' % (self.fs2, fs0, fs1), autocommit=True, ansi=True,
                                              trusted_connection='yes')
                except pyodbc.Error as err:
                    sqlstat0 = err.args[0]
                    sqlstat1 = err.args[1]
                    scmyerror = 1
                    QMessageBox.about(self, self.tr("Warning"), self.tr("\n%2").arg(sqlstat1))
                if scmyerror == 0:
                    cur = conn.cursor()

                    for tableName in input_file:


                        qtableName = tableName



                        qtableName = qtableName.split('.')
                        tableSchema = []
                        ntable = qtableName[1].strip()
                        nschema = qtableName[0].strip()

                        for fld in cur.columns(table=ntable, schema=nschema):
                            tableSchema.append('%s.%s' % (fld.column_name, fld.type_name))



                        for i in range(len(tableSchema)):
                            tcolumnName = (tableSchema[i])
                            tcolumnName = tcolumnName.split('.')
                            columnName = str(tcolumnName[0])
                            columnType = str(tcolumnName[1])

                            if 'NUMBER' in columnType:
                                columnType = "INT"
                                getColValTypes.append(columnName + ' ' + columnType)
                                getColNames.append(columnName)
                            elif 'STRING' in columnType:
                                columnType = "STRING"
                                getColValTypes.append(columnName + ' ' + columnType)
                                getColNames.append(columnName)
                            elif 'FIXED_CHAR' in columnType:
                                columnType = "STRING"
                                getColValTypes.append(columnName + ' ' + columnType)
                                getColNames.append(columnName)
                            elif 'TIMESTAMP' in columnType:
                                columnType = "TIMESTAMP"
                                getColValTypes.append(columnName + ' ' + columnType)
                                getColNames.append(columnName)
                            elif 'CLOB' in columnType:
                                columnType = "STRING"
                                getColValTypes.append(columnName + ' ' + columnType)
                                getColNames.append(columnName)
                            elif 'BLOB' in columnType:
                                columnType = "STRING"
                                getColValTypes.append(columnName + ' ' + columnType)
                                getColNames.append(columnName)
                            elif 'VARCHAR' in columnType:
                                columnType = "STRING"
                                getColValTypes.append(columnName + ' ' + columnType)
                                getColNames.append(columnName)
                            elif 'CHAR' in columnType:
                                columnType = "STRING"
                                getColValTypes.append(columnName + ' ' + columnType)
                                getColNames.append(columnName)
                            elif 'DATE' in columnType:
                                columnType = "DATE"
                                getColValTypes.append(columnName + ' ' + columnType)
                                getColNames.append(columnName)
                            elif 'RAW' in columnType:
                                columnType = "STRING"
                                getColValTypes.append(columnName + ' ' + columnType)
                                getColNames.append(columnName)
                            elif 'FLOAT' in columnType:
                                columnType = "DECIMAL"
                                getColValTypes.append(columnName + ' ' + columnType)
                                getColNames.append(columnName)
                            elif 'LONG' in columnType:
                                columnType = "DOUBLE"
                                getColValTypes.append(columnName + ' ' + columnType)
                                getColNames.append(columnName)
                            elif 'CLOB' in columnType:
                                columnType = "DOUBLE"
                                getColValTypes.append(columnName + ' ' + columnType)
                                getColNames.append(columnName)
                            elif 'INT' in columnType:
                                columnType = "INT"
                                getColValTypes.append(columnName + ' ' + columnType)
                                getColNames.append(columnName)
                            elif 'SMALLINT' in columnType:
                                columnType = "SMALLINT"
                                getColValTypes.append(columnName + ' ' + columnType)
                                getColNames.append(columnName)
                            elif 'BIGINT' in columnType:
                                columnType = "BIGINT"
                                getColValTypes.append(columnName + ' ' + columnType)
                                getColNames.append(columnName)
                            else:
                                getColValTypes.append(columnName + ' ' + "STRING")
                                getColNames.append(columnName)

                            #tableName = tableName.split('.')
                        createHiveStmt_file.write(
                                tablecreation[0] + ' ' + hiveORCSchema + qtableName[1] + '(' + ','.join(
                                    getColValTypes) + ')' + tablecreation[1] + r"'01'" + tablecreation[
                                    2] + ';' + '\n' + '\n')
                        Hiveingestion_file.write(
                                ingestionStmt[0] + ' ' + ingestionStmt[1] + hiveORCSchema + qtableName[1] +
                                ingestionStmt[2] + 'SELECT ' + ','.join(getColNames) + ingestionStmt[
                                    3] + hiveAVROSchema + qtableName[1] + ';' + '\n' + '\n')
                        getColValTypes = []
                        getColNames = []

                createHiveStmt_file.close()
                Hiveingestion_file.close()
                cur.close()

                sqlstatx = 'Creating Statments .click Ok'
                QMessageBox.about(self, self.tr("Message"), self.tr("\n%2").arg(sqlstatx))

                while self.completed < 100:
                    self.completed += 0.0001
                    self.be4bottom12.progress.setValue(self.completed)
                if self.rcheckvalue == 'ALL':
                    totalsrc = (len(self.hpprint))
                else:
                    totalsrc = 1
                self.bottom.printxt.setText("Total Table proceesed :{}".format(totalsrc))
                self.bottom.printxt3.setText("Total Hive Table Statment Created :{}".format(totalsrc))
                self.bottom.printxt4.setText("Total Number of Hive Insert Created :{}".format(totalsrc))

                sqlstat1 = 'Hive Table Statment Created saved to %s \n ' % (createHiveStmt)
                QMessageBox.about(self, self.tr("Message"), self.tr("\n%2").arg(sqlstat1))
                sqlstat2 = 'Hive Table Statment Created saved to %s \n ' % (createHiveingestion)
                QMessageBox.about(self, self.tr("Message"), self.tr("\n%2").arg(sqlstat2))


        else:
            sqlstat1 = 'Choose All the options,  Cant be empty'
            QMessageBox.warning(self, self.tr("Warning"),self.tr("\n%2").arg(sqlstat1))

    def close_application(self):
        """ Function to closes the window.

        	Returns:
        		(str) returns nothing but close the window.
        """
        choice = QtGui.QMessageBox.question(self, 'Leaving!',
                                            "Are you sure to close?",
                                            QtGui.QMessageBox.Yes | QtGui.QMessageBox.No)
        if choice == QtGui.QMessageBox.Yes:
            print("Closing Hive table generations!!!!")
            sys.exit()
        else:
            pass




