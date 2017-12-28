"""
	Autor : Payel Das
	Description : PyQt4 custom unload for all the DataSources including Hadoop.
	Created : Mar 10, 2017

"""

import sys
reload(sys)
sys.setdefaultencoding('utf8')
import pandas as pd
from PyQt4.QtGui import *
from PyQt4 import QtCore, QtGui
import pyodbc
import os
import os.path
import getpass
import time
import functools
from CheckComboBox import CheckComboBox
from splittergene import SplitGene


class UnloadWindow(QtGui.QMainWindow):
    """ Class definition for the custom H-Unload that can select datasouce and tables
    	options.
    """
    def __init__(self, parent=None):
        """ Initialization for HUnloadWindow class.

        	Args:
        		parent (QWidget): parent widget to which the HUnload is attached.
        """
        super(UnloadWindow, self).__init__(parent)
        self.win_widget = SplitterMain(self)
        widget = QtGui.QWidget()
        layout = QtGui.QVBoxLayout(widget)
        layout.addWidget(self.win_widget)
        self.setCentralWidget(widget)

        menubar = self.menuBar()
        extractAction = QtGui.QAction("&Leave the App!!!", self)
        extractAction.setShortcut("Ctrl+Q")

        extractAction.triggered.connect(self.win_widget.close_application)
        extractAction2 = QtGui.QAction("&Generated Hive Tables", self)
        extractAction2.setShortcut("Ctrl+E")
        extractAction2.triggered.connect(functools.partial(self.filecompare))
        extractAction3 = QtGui.QAction("&Help", self)
        extractAction3.setShortcut("Ctrl+H")
        extractAction3.triggered.connect(functools.partial(self.helpcop))

        fileMenu = menubar.addMenu('&Menu')

        fileMenu.addAction(extractAction)
        fileMenu.addAction(extractAction2)
        fileMenu.addAction(extractAction3)
        self.setGeometry(800, 600, 800, 600)
        self.setWindowTitle('H-UNLOAD beta')
        self.setWindowIcon(QtGui.QIcon('icon.ico'))
        self.center()
        self.show()

    def center(self):
        """ Function to get the geometry of the window.

        	Returns:
        		sets the current size of the PyQT window.
        """
        qr = self.frameGeometry()
        cp = QtGui.QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())

    def filecompare(self):
        """ Function to move to other fuction for generating to Hive tablestatments.

        	Returns:
        		Nothing moves to a new window .
        """
        self.close()
        self._new_window = SplitGene(self)
        self._new_window.show()

    def helpcop(self):
        """ Function to display some basics of this H-Unload window.

        	Returns:
        		 return the default text window.
        """
        stmt1 = 'You need to provide the userid and password '
        stmt2 = 'Then you need to select one of the data source listed .'
        stmt3 = 'Please make sure you have all the ODBC connetion already setup up.'
        stmt4 = 'For the Hadoop/ Postgre SQL select the appropiate box .'
        stmt5 = 'You have the options to select the table in the selected Datasource'
        stmt6 = 'once the table is selected it can be downloaded wether is Hadoop or any other datasource.'
        stmt7 = 'the table unloaded will saved in the csv format.'
        stmt8  = ''
        stmt9 =  ''

        sqlstat2 = ' %s \n %s \n %s \n %s \n %s \n %s ' \
                   '\n %s \n%s \n%s \n ' % (stmt1, stmt2 ,stmt3 ,stmt4 ,stmt5, stmt6, stmt7, stmt8 ,stmt9)
        QMessageBox.about(self, self.tr("Message"), self.tr("\n%2").arg(sqlstat2))

class SplitterMain(QtGui.QWidget):
    def __init__(self, parent):
        super(SplitterMain,self).__init__(parent)

        self.initUI()

    def initUI(self):
        """ Function to Initalize the all.

        	Returns:
        		This is for initalisation and setting up the display GUI. Looks for the Datasouce using PyODBC
        """
        self.hbox = QtGui.QHBoxLayout(self)

        self.fs2= ''
        self.fs3= ''
        self.lcheckvalue = 'NOHADOOP'
        self.rcheckvalue = 'NOHADOOP'
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

        self.topleft1.delsalutation.activated[str].connect(self.sourcecol)


        self.topright1 = QtGui.QFrame(self)
        self.topright1.setFrameShape(QtGui.QFrame.StyledPanel)
        self.topright1.setFixedSize(420,60)



        self.topleft12 = QtGui.QFrame(self)
        self.topleft12.setFrameShape(QtGui.QFrame.StyledPanel)
        self.topleft12.setFixedSize(420,60)


        self.topleft12.delsalutation = CheckComboBox(self.topleft12)
        self.topleft12.delsalutation.addItem("ALL                                    ")
        self.topleft12.delsalutation.move(21, 20)

        self.topleft12.done4left = QtGui.QPushButton('Done with Selection', self.topleft12)
        self.topleft12.done4left.move(211, 20)
        self.connect(self.topleft12.done4left, QtCore.SIGNAL('clicked()'), self.capturecol)




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
        self.be4bottom12.pushButton2 = QtGui.QPushButton('Hit to Unload', self.be4bottom12)
        self.be4bottom12.pushButton2.setFixedWidth(100)
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
        self.splitter12.addWidget(self.topleft12)
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
        """ Function to get Souce table.

        	Returns:
        		(str) return the table name that is selected.
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

            return self.fs2, self.fs0, self.fs1



    def targettable(self,text):
        """ Function to get target table though this function is not used in the H-unload.

        	Returns:
        		(str) returns table name target .
        """
        fs3 = text

        if len(str(self.scrollLayout2.username.text())) == 0:
            fs0 = getpass.getuser()
        else:
            fs0 = str(self.scrollLayout2.username.text())

        if len(str(self.scrollLayout2.password.text())) == 0:
            sqlstatx = 'Password cannot be blank'
            QMessageBox.about(self, self.tr("Warning"),self.tr("\n%2").arg(sqlstatx))
            fs1 = ' '
        else:
            fs1 = str(self.scrollLayout2.password.text())
        tmyerror = 0
        try:
            if self.rcheckvalue == 'NOHADOOP':
                conns = pyodbc.connect('DSN=%s;UID=%s;PWD=%s'%(fs3,fs0,fs1))
            else:
                conns = pyodbc.connect('DSN=%s;UID=%s;PWD=%s'%(fs3,fs0,fs1),autocommit= True,ansi= True,trusted_connection='yes')
        except pyodbc.Error as err:
            sqlstat0 = err.args[0]
            sqlstat1 = err.args[1]
            tmyerror = 1
            QMessageBox.about(self, self.tr("Warning"),self.tr("\n%2").arg(sqlstat1))

        if tmyerror == 0:
            cursor1=conns.cursor()
            thpprint = []
            for row in cursor1.tables(tableType='TABLE'):
                thpprint.append('%s.%s'%(row.table_schem ,row.table_name))
            self.topleft1.delsalutations = thpprint
            for i in range(len(thpprint)):
                self.topright1.delsalutation.addItem(thpprint[i])
            self.topright1.delsalutation.move(20, 10)
            self.fs3 = fs3
            return self.fs3

    def sourcecol(self,text):
        """ Function to get the selected source coloumn for the selectect table.

        	Returns:
        		(str) return the columns for the selectec table.
        """
        fs2 = self.fs2

        if len(str(self.scrollLayout2.username.text())) == 0:
            fs0 = getpass.getuser()
        else:
            fs0 = str(self.scrollLayout2.username.text())

        if len(str(self.scrollLayout2.password.text())) == 0:
            sqlstatx = 'Password cannot be blank'
            QMessageBox.about(self, self.tr("Warning"),self.tr("\n%2").arg(sqlstatx))
            fs1 = ' '
        else:
            fs1 = str(self.scrollLayout2.password.text())
        newnamecnt = 0
        names = text.split('.')
        for name in names:
            if newnamecnt == 0:
                nschema=str(name)
                newnamecnt +=1
            else:
                ntable=str(name)

        scmyerror = 0
        hpprint = []

        try:
            if self.lcheckvalue == 'NOHADOOP':
                conn = pyodbc.connect('DSN=%s;UID=%s;PWD=%s'%(fs2,fs0,fs1))
            else:
                conn = pyodbc.connect('DSN=%s;UID=%s;PWD=%s'%(fs2,fs0,fs1),autocommit= True,ansi= True,trusted_connection='yes')
        except pyodbc.Error as err:
            sqlstat0 = err.args[0]
            sqlstat1 = err.args[1]
            scmyerror = 1
            QMessageBox.about(self, self.tr("Warning"),self.tr("\n%2").arg(sqlstat1))
        if scmyerror == 0:
            cursor1=conn.cursor()
            hpprint = []
            for fld in cursor1.columns(table=ntable, schema=nschema):
                hpprint.append('%s'%(fld.column_name))
            self.topleft12.delsalutations = hpprint
            for i in range(len(hpprint)):
                self.topleft12.delsalutation.addItem(hpprint[i])

        self.sourcetabnam = text
        return self.sourcetabnam


    def targetcol(self,text):
        """ Function to get the columns from the selected target table.Though the this is not used .

        	Returns:
        		(str) return the list of columns name for the target table.
        """
        fs2 = self.fs3

        if len(str(self.scrollLayout2.username.text())) == 0:
            fs0 = getpass.getuser()
        else:
            fs0 = str(self.scrollLayout2.username.text())

        if len(str(self.scrollLayout2.password.text())) == 0:
            sqlstatx = 'Password cannot be blank'
            QMessageBox.about(self, self.tr("Warning"),self.tr("\n%2").arg(sqlstatx))
            fs1 = ' '
        else:
            fs1 = str(self.scrollLayout2.password.text())
        newnamecnt = 0
        names = text.split('.')
        for name in names:
            if newnamecnt == 0:
                nschema=str(name)
                newnamecnt +=1
            else:
                ntable=str(name)

        hpprint = []

        srcmyerror = 0

        try:
            if self.rcheckvalue == 'NOHADOOP':
                conn = pyodbc.connect('DSN=%s;UID=%s;PWD=%s'%(fs2,fs0,fs1))
            else:
                conn = pyodbc.connect('DSN=%s;UID=%s;PWD=%s'%(fs2,fs0,fs1),autocommit= True,ansi= True,trusted_connection='yes')
        except pyodbc.Error as err:
            sqlstat0 = err.args[0]
            sqlstat1 = err.args[1]
            srcmyerror = 1
            QMessageBox.about(self, self.tr("Warning"),self.tr("\n%2").arg(sqlstat1))
        if srcmyerror == 0:
            cursor1=conn.cursor()
            hpprint = []
            for fld in cursor1.columns(table=ntable, schema=nschema):
                hpprint.append('%s'%(fld.column_name))
            self.topright12.delsalutations = hpprint
            for i in range(len(hpprint)):
                self.topright12.delsalutation.addItem(hpprint[i])

        self.targetabnam = text
        return self.targetabnam

    def capturecol(self):
        """ Function to obtain captured columns.

        	Returns:
        		(str) return the cpatured columns.
        """
        selectedItems = self.topleft12.delsalutation.checkedItems()
        colprint = []
        for item in selectedItems:
            colprint.append(str(item))


        if colprint[0] != 'ALL':
            self.sourc1col = colprint

        return self.sourc1col

    def capturecol2(self):
        """ Function to get the list of captured columns.

        	Returns:
        		(str) return the captured columns.
        """
        selectedItems = self.topright12.delsalutation.checkedItems()
        colprint = []
        for item in selectedItems:
            colprint.append(str(item))

        if colprint[0] != 'ALL':
            self.sourc1co2 = colprint

        return self.sourc1co2

    def leftchangeTitle(self, state):
        """ Function to get the if hadoop is checked for the source datasource.

        	Returns:
        		(str) return the selection details for hadoop or not.
        """
        if state == QtCore.Qt.Checked:
            self.lcheckvalue = 'HADOOP'
        else:
            self.lcheckvalue = 'NOHADOOP'
        return self.lcheckvalue

    def rightchangeTitle(self, state):
        """ Function to get if hapoop is checked for the target datasource.

        	Returns:
        		(str) return the selection details for hadoop or not.
        """
        if state == QtCore.Qt.Checked:
            self.rcheckvalue = 'HADOOP'
        else:
            self.rcheckvalue = 'NOHADOOP'
        return self.rcheckvalue

    def recognize(self):
        """ Function to runs when the user hits the procees button for unload.

        	Returns:
        		(str) returns and save the unload data to file.
        """

        self.completed = 0
        fs0 = self.fs0
        fs1 = self.fs1


        if  self.fs2 != '' and self.sourcetabnam != '':
            finerror = 0

            try:
                if self.lcheckvalue == 'NOHADOOP':
                    conn = pyodbc.connect('DSN=%s;UID=%s;PWD=%s'%(self.fs2,fs0,fs1))
                else:
                    conn = pyodbc.connect('DSN=%s;UID=%s;PWD=%s'%(self.fs2,fs0,fs1),autocommit= True,ansi= True,trusted_connection='yes')
            except pyodbc.Error as err:
                sqlstat0 = err.args[0]
                sqlstat1 = err.args[1]
                finerror = 1
                QMessageBox.about(self, self.tr("Warning"),self.tr("\n%2").arg(sqlstat1))
            if finerror == 0:
                sqlsourcecnt = ('SELECT count(*) from %s' %(self.sourcetabnam))
                try:
                    dfs1 = (pd.read_sql(sqlsourcecnt, conn))
                except pyodbc.Error as err:
                    print("\nAn error occurred. Error number {0}: {1}.".format(err.args[0],err.args[1]))
                    QMessageBox.about(self, self.tr("Warning"),self.tr("\n%1").format(err.args[0],err.args[1]))
            if dfs1.iloc[0,0] >= 900000:
                print "There is a huge data , you may run out of memory so please stop"
                #looplogic implement
            else:
                sqlsource = ('SELECT * from %s' %(self.sourcetabnam))
                try:
                    dfstable1 = (pd.read_sql(sqlsource, conn))
                    print("dfstable1 dimension :{}".format(dfstable1.shape))
                except pyodbc.Error as err:
                    sqlstat0 = err.args[0]
                    sqlstat1 = err.args[1]
                    QMessageBox.about(self, self.tr("Warning"),self.tr("\n%2").arg(sqlstat1))
            keycol = str(self.sourc1co2)
            while self.completed < 100:
                self.completed += 0.0001
                self.be4bottom12.progress.setValue(self.completed)

            totalsrc = (dfstable1.shape)

            self.bottom.printxt3.setText("Total Number of Rows Unloaded :{}".format(totalsrc[0]))

            file_name =  time.strftime("%Y%m%d%H%M%S") + 'data.csv'
            dfstable1.to_csv(file_name, sep=',' ,mode = 'w')
            sqlstat1 ='Total Number of Rows Unloaded saved to %s \n '%( file_name)
            QMessageBox.about(self, self.tr("Message"),self.tr("\n%2").arg(sqlstat1))



        else:
            sqlstat1 = 'Choose All the options,  Cant be empty'
            QMessageBox.warning(self, self.tr("Warning"),self.tr("\n%2").arg(sqlstat1))

    def close_application(self):
        """ Function to close the window.

        	Returns:
        		(nothing but close the window.
        """
        choice = QtGui.QMessageBox.question(self, 'Leaving !',
                                            "Are you sure to close?",
                                            QtGui.QMessageBox.Yes | QtGui.QMessageBox.No)
        if choice == QtGui.QMessageBox.Yes:
            print("Unload function is closed!!!!")
            sys.exit()
        else:
            pass




