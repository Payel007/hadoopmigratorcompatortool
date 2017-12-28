"""
	Autor : Payel Das
	Description : PyQt4 custom Hadoop Comprator.
	Created : Mar 10, 2017
"""

import sys
reload(sys)
sys.setdefaultencoding('utf8')
import pandas as pd
from PyQt4.QtCore import *
from PyQt4.QtGui import *
from PyQt4 import QtCore, QtGui
import pyodbc
import os
import os.path
import getpass
import time
import functools
import psutil
import numpy as np
from CheckComboBox import CheckComboBox
from tableunload import UnloadWindow
from splittergene import SplitGene
from matplotlib.backends.backend_qt4agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from PyQt4.QtGui import QWidget
import logging
logging.getLogger().setLevel(logging.INFO)

class MainWindow(QtGui.QMainWindow):
    """ Class definition for the custom H-Migrator Compartor that can select datasouce and tables
    	options.
    """
    def __init__(self, parent=None):
        """ Initialization for HMigrator compare class.

        	Args:
        		parent (QWidget): parent widget for Hadoop compare .
        """
        super(MainWindow, self).__init__(parent)
        self.win_widget = SplitterMain(self)
        widget = QtGui.QWidget()
        layout = QtGui.QVBoxLayout(widget)
        layout.addWidget(self.win_widget)
        self.setCentralWidget(widget)
        logging.basicConfig(level=logging.INFO)
        logging.getLogger()

        menubar = self.menuBar()
        extractAction = QtGui.QAction("&Leave the App", self)
        extractAction.setShortcut("Ctrl+Q")

        extractAction.triggered.connect(self.win_widget.close_application)
        extractAction1 = QtGui.QAction("&Table Unload", self)
        extractAction1.setShortcut("Ctrl+W")
        # extractAction1.triggered.connect(self.table)
        extractAction1.triggered.connect(functools.partial(self.table))
        extractAction2 = QtGui.QAction("&Generated Hive Tables", self)
        extractAction2.setShortcut("Ctrl+E")
        extractAction2.triggered.connect(functools.partial(self.filecompare))
        extractAction3 = QtGui.QAction("&Help", self)
        extractAction3.setShortcut("Ctrl+H")
        extractAction3.triggered.connect(functools.partial(self.helpcompare))

        fileMenu = menubar.addMenu('&Menu')
        #menubar.addMenu('&Help')
        fileMenu.addAction(extractAction)
        fileMenu.addAction(extractAction1)
        fileMenu.addAction(extractAction2)
        fileMenu.addAction(extractAction3)

        self.setGeometry(800, 600, 800, 600)
        self.setWindowTitle('H-Migrator beta')
        self.setWindowIcon(QtGui.QIcon('icon.ico'))
        self.center()
        self.show()

    def table(self):
        self.close()
        self._new_window = UnloadWindow(self)
        self._new_window.show()

    def filecompare(self):
        self.close()
        self._new_window = SplitGene(self)
        self._new_window.show()

    def helpcompare(self):
        """ Function to display some basics of this H-Unload window.

        	Returns:
        		 return the default text window.
        """
        stmt1 = 'You need to provide the userid and password '
        stmt2 = 'Then you need to select one of the data source listed .'
        stmt3 = 'Please make sure you have all the ODBC connetion already setup up.'
        stmt4 = 'For the Hadoop/ Postgre SQL select the appropiate box .'
        stmt5 = 'You have the options to compare two tables across any source of database'
        stmt6 = 'weather is Hadoop or any other datasource.'
        stmt7 = 'You have a flexiblity to save the compared datathe table unloaded will saved in the csv format.'
        stmt8  = 'There is list of details and pictorial repsenations'
        stmt9 =  'The transpose compare will be avaliable in next version'

        sqlstat2 = ' %s \n %s \n %s \n %s \n %s \n %s ' \
                   '\n %s \n%s \n%s \n ' % (stmt1, stmt2 ,stmt3 ,stmt4 ,stmt5, stmt6, stmt7, stmt8 ,stmt9)
        QMessageBox.about(self, self.tr("Message"), self.tr("\n%2").arg(sqlstat2))



    def center(self):
        qr = self.frameGeometry()
        cp = QtGui.QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())


class SplitterMain(QtGui.QWidget):
    """ Class definition for the custom compare of two table  that can select datasouce and tables
    	options and compare the selected traget source and tables .
    """
    def __init__(self, parent):
        super(SplitterMain, self).__init__(parent)

        self.initUI()

    def initUI(self):
        self.hbox = QtGui.QHBoxLayout(self)

        self.fs2 = ''
        self.fs3 = ''
        self.lcheckvalue = 'NOHADOOP'
        self.rcheckvalue = 'NOHADOOP'
        self.sourcetabnam = ''
        self.targetabnam = ''
        self.sourc1col = ''
        self.sourc1co2 = ''
        self.mergecolkey = ' '
        self.hpprint = []
        self.rect = 0
        self.rect1 = 0
        self.rect2 = 0
        self.rect3 = 0

        self.topleft = QtGui.QFrame(self)
        self.topleft.setFrameShape(QtGui.QFrame.StyledPanel)
        self.topleft.setFixedSize(420, 60)

        sources = pyodbc.dataSources()
        foods = sources.keys()
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
        # print self.fs2

        self.topright = QtGui.QFrame(self)
        self.topright.setFrameShape(QtGui.QFrame.StyledPanel)
        self.topright.setFixedSize(420, 60)

        sources = pyodbc.dataSources()
        foods = sources.keys()
        foods.sort()
        self.topright.salutations = foods
        self.topright.salutation = QComboBox(self.topright)
        self.topright.salutation.addItem("Select Target Datasource")

        for i in range(len(foods)):
            self.topright.salutation.addItem(foods[i])

        self.topright.salutation.move(28, 20)
        self.topright.hadcheck = QtGui.QCheckBox('IsHadoop/Greenplum', self.topright)
        self.topright.hadcheck.move(210, 20)
        self.topright.hadcheck.stateChanged.connect(self.rightchangeTitle)
        self.topright.salutation.activated[str].connect(self.targettable)

        self.bottom = QtGui.QFrame(self)
        self.bottom.setFrameShape(QtGui.QFrame.StyledPanel)

        self.bottom.setFixedSize(500, 180)

        self.bottom.printxt = QtGui.QLabel(self.bottom)

        self.bottom.printxt.setText("                                                ")
        self.bottom.printxt.setStyleSheet(
            'font-family: Courier New;font-style: normal;font-size: 8pt;font-weight: bold;color: blue')
        self.bottom.printxt.move(28, 20)
        self.bottom.printxt2 = QtGui.QLabel(self.bottom)
        self.bottom.printxt2.setText("                                               ")
        self.bottom.printxt2.setStyleSheet(
            'font-family: Courier New;font-style: normal;font-size: 8pt;font-weight: bold;color: blue')
        self.bottom.printxt2.move(28, 40)
        self.bottom.printxt3 = QtGui.QLabel(self.bottom)
        self.bottom.printxt3.setText("                                               ")
        self.bottom.printxt3.setStyleSheet(
            'font-family: Courier New;font-style: normal;font-size: 8pt;font-weight: bold;color: blue')
        self.bottom.printxt3.move(28, 60)
        self.bottom.printxt4 = QtGui.QLabel(self.bottom)
        self.bottom.printxt4.setText("                                               ")
        self.bottom.printxt4.setStyleSheet(
            'font-family: Courier New;font-style: normal;font-size: 8pt;font-weight: bold;color: blue')
        self.bottom.printxt4.move(28, 80)

        self.fft_frame = QtGui.QFrame(self)
        self.fft_frame.setFrameShape(QtGui.QFrame.StyledPanel)

        self.fft_frame.setFixedSize(340, 180)
        self.fft_frame.graph_view = GraphView('fftFrame', 'DataAnalysis:', 'FFT Transform of Signal', self.fft_frame)
        x = np.array([[self.rect, self.rect1, self.rect2, self.rect3]])
        self.df = pd.DataFrame(x, columns=['TS', 'TT', 'NM', 'M'])
        self.fft_frame.graph_view.update_graph(self.df)

        self.topleft1 = QtGui.QFrame(self)
        self.topleft1.setFrameShape(QtGui.QFrame.StyledPanel)
        self.topleft1.setFixedSize(420, 60)

        self.topleft1.delsalutation = QComboBox(self.topleft1)
        self.topleft1.delsalutation.addItem("Select Source Tablename")
        self.topleft1.delsalutation.move(21, 20)
        # fs2 = self.fs2
        self.topleft1.delsalutation.activated[str].connect(self.sourcecol)

        self.topright1 = QtGui.QFrame(self)
        self.topright1.setFrameShape(QtGui.QFrame.StyledPanel)
        self.topright1.setFixedSize(420, 60)

        self.topright1.delsalutation = QComboBox(self.topright1)
        self.topright1.delsalutation.addItem("Select Target Tablename")
        self.topright1.delsalutation.move(30, 20)
        self.topright1.delsalutation.activated[str].connect(self.targetcol)

        self.topleft12 = QtGui.QFrame(self)
        self.topleft12.setFrameShape(QtGui.QFrame.StyledPanel)
        self.topleft12.setFixedSize(420, 60)


        self.topleft12.delsalutation = CheckComboBox(self.topleft12)
        self.topleft12.delsalutation.addItem("ALL                                    ")
        self.topleft12.delsalutation.move(21, 20)

        self.topleft12.done4left = QtGui.QPushButton('Done with Source', self.topleft12)
        self.topleft12.done4left.move(211, 20)
        self.connect(self.topleft12.done4left, QtCore.SIGNAL('clicked()'), self.capturecol)

        self.topright12 = QtGui.QFrame(self)
        self.topright12.setFrameShape(QtGui.QFrame.StyledPanel)
        self.topright12.setFixedSize(420, 60)


        self.topright12.delsalutation = CheckComboBox(self.topright12)
        self.topright12.delsalutation.addItem("ALL                                  ")
        self.topright12.delsalutation.move(30, 20)

        self.topright12.done4left = QtGui.QPushButton('Done with Target', self.topright12)
        self.topright12.done4left.move(211, 20)
        self.connect(self.topright12.done4left, QtCore.SIGNAL('clicked()'), self.capturecol2)

        self.scrollLayout2 = QtGui.QFormLayout()
        self.scrollLayout2.username = QtGui.QLineEdit()
        self.scrollLayout2.username.setPlaceholderText(getpass.getuser())
        self.scrollLayout2.password = QtGui.QLineEdit()
        self.scrollLayout2.password.setEchoMode(QtGui.QLineEdit.Password)
        self.scrollLayout2.username.setFixedSize(80, 20)
        self.scrollLayout2.password.setFixedSize(80, 20)
        self.scrollLayout2.addRow("Username", self.scrollLayout2.username)

        self.scrollLayout2.addRow("Password", self.scrollLayout2.password)
        self.scrollLayout2.setFormAlignment(QtCore.Qt.AlignRight)


        self.scrollWidget2 = QtGui.QWidget()
        self.scrollWidget2.setLayout(self.scrollLayout2)

        self.scrollArea2 = QtGui.QScrollArea()
        self.scrollArea2.setWidgetResizable(True)
        self.scrollArea2.setFixedSize(848, 120)
        self.scrollArea2.setWidget(self.scrollWidget2)

        self.be4bottom12 = QtGui.QFrame(self)
        self.be4bottom12.setFrameShape(QtGui.QFrame.StyledPanel)
        self.be4bottom12.setFixedSize(850, 40)
        self.be4bottom12.pushButton2 = QtGui.QPushButton('Hit to Compare', self.be4bottom12)
        self.be4bottom12.pushButton2.setFixedWidth(100)
        self.be4bottom12.pushButton2.move(300, 10)
        self.be4bottom12.pushButton2.clicked.connect(lambda: self.recognize())
        self.be4bottom12.pushButton3 = QtGui.QPushButton('Transpose Compare', self.be4bottom12)
        self.be4bottom12.pushButton3.setFixedWidth(120)
        self.be4bottom12.pushButton3.move(420, 10)
        self.be4bottom12.pushButton3.clicked.connect(lambda:self.transpose())
        self.be4bottom12.progress = QtGui.QProgressBar(self.be4bottom12)

        self.be4bottom12.progress.setGeometry(100, 80, 250, 20)
        self.be4bottom12.progress.move(600, 10)

        self.splitter1 = QtGui.QSplitter(QtCore.Qt.Horizontal)
        self.splitter1.addWidget(self.topleft)

        self.splitter1.addWidget(self.topright)


        self.splitter11 = QtGui.QSplitter(QtCore.Qt.Horizontal)
        self.splitter11.addWidget(self.topleft1)
        self.splitter11.addWidget(self.topright1)

        self.splitter12 = QtGui.QSplitter(QtCore.Qt.Horizontal)
        self.splitter12.addWidget(self.topleft12)
        self.splitter12.addWidget(self.topright12)

        self.splitter13 = QtGui.QSplitter(QtCore.Qt.Horizontal)

        self.splitter13.addWidget(self.bottom)
        self.splitter13.addWidget(self.fft_frame)

        self.splitter2 = QtGui.QSplitter(QtCore.Qt.Vertical)
        self.splitter2.addWidget(self.scrollArea2)
        self.splitter2.addWidget(self.splitter1)
        self.splitter2.addWidget(self.splitter11)
        self.splitter2.addWidget(self.splitter12)
        self.splitter2.addWidget(self.be4bottom12)
        self.splitter2.addWidget(self.splitter13)

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

    def update_figure(self, rect0, rect1, rect2, rect3):
        self.rect0 = rect0
        self.rect1 = rect1
        self.rect2 = rect2
        self.rect3 = rect3

        x = np.array([[self.rect0, self.rect1, self.rect2, self.rect3]])
        self.df = pd.DataFrame(x, columns=['TS', 'TT', 'NM', 'M'])

        self.fft_frame.graph_view.update_graph(self.df)

    def sourcetable(self, text):
        """ Function to obtain the list of avaliable datasouces for which user run compare.

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

        # fs1 = str(self.scrollLayout2.password.text())


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

    def targettable(self, text):
        """ Function to obtain the list of avaliable target datasouces for user can compare.

        	Returns:
        		(str) list of datasouces (hadoop as well as no hadoop).
        """
        fs3 = text

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
        tmyerror = 0
        try:
            if self.rcheckvalue == 'NOHADOOP':
                conns = pyodbc.connect('DSN=%s;UID=%s;PWD=%s' % (fs3, fs0, fs1))
            else:
                conns = pyodbc.connect('DSN=%s;UID=%s;PWD=%s' % (fs3, fs0, fs1), autocommit=True, ansi=True,
                                       trusted_connection='yes')
        except pyodbc.Error as err:
            sqlstat0 = err.args[0]
            sqlstat1 = err.args[1]
            tmyerror = 1
            QMessageBox.about(self, self.tr("Warning"), self.tr("\n%2").arg(sqlstat1))

        if tmyerror == 0:
            cursor1 = conns.cursor()
            thpprint = []
            for row in cursor1.tables(tableType='TABLE'):
                thpprint.append('%s.%s' % (row.table_schem, row.table_name))
            self.topleft1.delsalutations = thpprint
            for i in range(len(thpprint)):
                self.topright1.delsalutation.addItem(thpprint[i])
            self.topright1.delsalutation.move(20, 10)
            self.fs3 = fs3
            return self.fs3

    def sourcecol(self, text):
        """ Function to obtain the selected table by the user.

        	Returns:
        		(str) return the selected source tablename.
        """
        fs2 = self.fs2

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
        newnamecnt = 0
        names = text.split('.')
        for name in names:
            if newnamecnt == 0:
                nschema = str(name)
                newnamecnt += 1
            else:
                ntable = str(name)

        scmyerror = 0
        hpprint = []

        try:
            if self.lcheckvalue == 'NOHADOOP':
                conn = pyodbc.connect('DSN=%s;UID=%s;PWD=%s' % (fs2, fs0, fs1))
            else:
                conn = pyodbc.connect('DSN=%s;UID=%s;PWD=%s' % (fs2, fs0, fs1), autocommit=True, ansi=True,
                                      trusted_connection='yes')
        except pyodbc.Error as err:
            sqlstat0 = err.args[0]
            sqlstat1 = err.args[1]
            scmyerror = 1
            QMessageBox.about(self, self.tr("Warning"), self.tr("\n%2").arg(sqlstat1))
        if scmyerror == 0:
            cursor1 = conn.cursor()
            hpprint = []

            self.topleft12.delsalutations = hpprint
            self.topleft12.delsalutation.clear()
            self.topleft12.delsalutation.addItem("ALL")

            for fld in cursor1.columns(table=ntable, schema=nschema):
                hpprint.append('%s' % (fld.column_name))
            self.topleft12.delsalutations = hpprint
            print self.topleft12.delsalutations
            for i in range(len(hpprint)):
                self.topleft12.delsalutation.addItem(hpprint[i])

        self.sourcetabnam = text

        return self.sourcetabnam

    def targetcol(self, text):
        """ Function to obtain the selected target table by the user.

        	Returns:
        		(str) return the selected target tablename.
        """
        fs2 = self.fs3

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
        newnamecnt = 0
        names = text.split('.')
        for name in names:
            if newnamecnt == 0:
                nschema = str(name)
                newnamecnt += 1
            else:
                ntable = str(name)

        hpprint = []

        srcmyerror = 0

        try:
            if self.rcheckvalue == 'NOHADOOP':
                conn = pyodbc.connect('DSN=%s;UID=%s;PWD=%s' % (fs2, fs0, fs1))
            else:
                conn = pyodbc.connect('DSN=%s;UID=%s;PWD=%s' % (fs2, fs0, fs1), autocommit=True, ansi=True,
                                      trusted_connection='yes')
        except pyodbc.Error as err:
            sqlstat0 = err.args[0]
            sqlstat1 = err.args[1]
            srcmyerror = 1
            QMessageBox.about(self, self.tr("Warning"), self.tr("\n%2").arg(sqlstat1))
        if srcmyerror == 0:
            cursor1 = conn.cursor()
            hpprint = []
            self.topright12.delsalutations = hpprint
            self.topright12.delsalutation.clear()
            self.topright12.delsalutation.addItem("ALL")
            for fld in cursor1.columns(table=ntable, schema=nschema):
                hpprint.append('%s' % (fld.column_name))
            self.topright12.delsalutations = hpprint
            for i in range(len(hpprint)):
                self.topright12.delsalutation.addItem(hpprint[i])

        self.targetabnam = text
        self.hpprint = hpprint
        return self.targetabnam, self.hpprint

    def capturecol(self):
        selectedItems = self.topleft12.delsalutation.checkedItems()
        colprint = []
        for item in selectedItems:
            colprint.append(str(item))
            print item
        if colprint:
            print colprint
            if colprint[0] != 'ALL':
                self.sourc1col = ', '.join(colprint)
            else:
                self.sourc1col = '*'

            return self.sourc1col
        else:
            print 'reselect'

            # self.sourc1col = colprint

    def capturecol2(self):
        selectedItems = self.topright12.delsalutation.checkedItems()

        colprint = []
        for item in selectedItems:
            colprint.append(str(item))
            print item
        print colprint
        if colprint:
            if colprint[0] != 'ALL':
                self.sourc1co2 = ', '.join(colprint)
            else:
                self.sourc1co2 = '*'
            print self.sourc1co2
            nonselectedItems = self.hpprint
            print nonselectedItems
        colprint = []
        for item in selectedItems:
            colprint.append(str(item))
        if colprint:
            if colprint[0] != 'ALL':
                self.mergecolkey = colprint
            else:
                newcolprint = []
                for item in nonselectedItems:
                    newcolprint.append(str(item))
                self.mergecolkey = newcolprint


        return self.sourc1co2, self.mergecolkey

    def leftchangeTitle(self, state):
        if state == QtCore.Qt.Checked:
            self.lcheckvalue = 'HADOOP'
        else:
            self.lcheckvalue = 'NOHADOOP'
        return self.lcheckvalue

    def rightchangeTitle(self, state):
        if state == QtCore.Qt.Checked:
            self.rcheckvalue = 'HADOOP'
        else:
            self.rcheckvalue = 'NOHADOOP'
        return self.rcheckvalue

    def recognize(self):
        """ Function to creates the compare two tables data of hadoop or other data source.

        	Returns:
        		Save the difference in the table in two datasystems.
        """
        self.completed = 0
        targetnolimitflag = 'OFF'
        sourcenolimitflag = 'OFF'
        fs0 = self.fs0
        fs1 = self.fs1

        if self.fs3 != '' and self.fs2 != '' and self.sourcetabnam != '' and self.targetabnam != '':
            finerror = 0
            dfstable1 = []
            dfstable2 = []

            try:
                if self.lcheckvalue == 'NOHADOOP':
                    conn = pyodbc.connect('DSN=%s;UID=%s;PWD=%s' % (self.fs2, fs0, fs1))
                else:
                    conn = pyodbc.connect('DSN=%s;UID=%s;PWD=%s' % (self.fs2, fs0, fs1), autocommit=True, ansi=True,
                                          trusted_connection='yes')
            except pyodbc.Error as err:
                sqlstat0 = err.args[0]
                sqlstat1 = err.args[1]
                finerror = 1
                QMessageBox.about(self, self.tr("Warning"), self.tr("\n%2").arg(sqlstat1))
            if finerror == 0:
                sqlsourcecnt = ('SELECT count(*) from %s' % (self.sourcetabnam))
                try:
                    dfs1 = (pd.read_sql(sqlsourcecnt, conn))
                except pyodbc.Error as err:
                    print("\nAn error occurred. Error number {0}: {1}.".format(err.args[0], err.args[1]))
                    QMessageBox.about(self, self.tr("Warning"), self.tr("\n%1").format(err.args[0], err.args[1]))
            if dfs1.iloc[0, 0] >= 1000000:
                print "Hello Logic looper1"
                w = QWidget()
                loopermsg01 = 'Looks like this is huge to me '
                loopermsg02 = dfs1.iloc[0, 0]
                QMessageBox.about(self, self.tr("Warning"), self.tr("%1\n%2").arg(loopermsg01).arg(loopermsg02))
                result = QMessageBox.question(w, 'Message',
                                              "The data is huge on source , Do you want to clip is short?",
                                              QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
                # loppermsg01 = 'The data is huge , Do you want to clip is short'
                if result == QMessageBox.Yes:
                    text, ok = QtGui.QInputDialog.getText(self, 'Input Dialog', 'Enter your limit:')
                    if ok:
                        limit = (str(text))
                        sourcenonlimitflag = 'OFF'
                        if self.lcheckvalue == 'HADOOP':
                            sqlsource = "SELECT %s FROM %s limit %s" % (self.sourc1col, self.sourcetabnam, limit)
                            try:
                                dfstable1 = (pd.read_sql(sqlsource, conn))
                                print("dfstable1 dimension :{}".format(dfstable1.shape))
                            except pyodbc.Error as err:
                                sqlstat0 = err.args[0]
                                sqlstat1 = err.args[1]
                                QMessageBox.about(self, self.tr("Warning"), self.tr("\n%2").arg(sqlstat1))
                        else:
                            sqlsource = "SELECT %s FROM %s Fetch first %s rows only" % (
                            self.sourc1col, self.sourcetabnam, limit)
                            try:
                                dfstable1 = (pd.read_sql(sqlsource, conn))
                                print("dfstable1 dimension :{}".format(dfstable1.shape))
                            except pyodbc.Error as err:
                                sqlstat0 = err.args[0]
                                sqlstat1 = err.args[1]
                                QMessageBox.about(self, self.tr("Warning"), self.tr("\n%2").arg(sqlstat1))

                    print limit

                else:

                    sqlstat1 = 'You may run out of memory , but its worth a shot!! Current Memory avaliable in GB'
                    sqlstat2 = psutil.virtual_memory().free / float(1000000000)
                    sqlstat3 = str(round(sqlstat2, 2))
                    QMessageBox.about(self, self.tr("Warning"), self.tr("%1\n%2").arg(sqlstat1).arg(sqlstat3))
                    sourcenolimitflag = 'ON'
                    # looplogic implement
            else:
                sqlsource = ('SELECT %s from %s' % (self.sourc1col, self.sourcetabnam))
                try:
                    dfstable1 = (pd.read_sql(sqlsource, conn))
                    print("dfstable1 dimension :{}".format(dfstable1.shape))
                except pyodbc.Error as err:
                    sqlstat0 = err.args[0]
                    sqlstat1 = err.args[1]
                    QMessageBox.about(self, self.tr("Warning"), self.tr("\n%2").arg(sqlstat1))
            # this is for the Target database to fetch it from
            try:
                if self.rcheckvalue == 'NOHADOOP':
                    connx = pyodbc.connect('DSN=%s;UID=%s;PWD=%s' % (self.fs3, fs0, fs1))
                else:
                    connx = pyodbc.connect('DSN=%s;UID=%s;PWD=%s' % (self.fs3, fs0, fs1), autocommit=True, ansi=True,
                                           trusted_connection='yes')
            except pyodbc.Error as err:
                sqlstat0 = err.args[0]
                sqlstat1 = err.args[1]
                finerror = 1
                QMessageBox.about(self, self.tr("Warning"), self.tr("\n%2").arg(sqlstat1))
            if finerror == 0:
                sqltargetcnt = ('SELECT count(*) from %s' % (self.targetabnam))
                try:
                    dfs2 = (pd.read_sql(sqltargetcnt, connx))
                except pyodbc.Error as err:
                    print("\nAn error occurred. Error number {0}: {1}.".format(err.args[0], err.args[1]))
                    QMessageBox.about(self, self.tr("Warning"), self.tr("\n%1").format(err.args[0], err.args[1]))
            if dfs2.iloc[0, 0] >= 100000:
                print "Hello Logic looper2"
                print "Hello Logic looper2"
                w = QWidget()
                loopermsg01 = 'Looks like this is huge to me, if you have opted to go for full select on source ,its mandatory you do the same  '
                loopermsg02 = dfs1.iloc[0, 0]
                QMessageBox.about(self, self.tr("Warning"), self.tr("%1\n%2").arg(loopermsg01).arg(loopermsg02))
                result = QMessageBox.question(w, 'Message',
                                              "The data is huge on Target , Do you want to clip is short?",
                                              QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
                # loppermsg01 = 'The data is huge , Do you want to clip is short'
                if result == QMessageBox.Yes:
                    text, ok = QtGui.QInputDialog.getText(self, 'Input Dialog', 'Enter your limit:')
                    if ok:
                        limit = (str(text))
                        nonlimitflag = 'OFF'
                        if self.lcheckvalue == 'HADOOP':
                            sqlsource = "SELECT %s FROM %s limit %s" % (self.sourc1co2, self.targetabnam, limit)
                            try:
                                dfstable2 = (pd.read_sql(sqlsource, conn))
                                print("dfstable1 dimension :{}".format(dfstable2.shape))
                            except pyodbc.Error as err:
                                sqlstat0 = err.args[0]
                                sqlstat1 = err.args[1]
                                QMessageBox.about(self, self.tr("Warning"), self.tr("\n%2").arg(sqlstat1))
                        else:
                            sqlsource = "SELECT %s  FROM %s Fetch first %s rows only" % (
                            self.sourc1co2, self.targetabnam, limit)
                            try:
                                dfstable2 = (pd.read_sql(sqlsource, conn))
                                print("dfstable2 dimension :{}".format(dfstable2.shape))
                            except pyodbc.Error as err:
                                sqlstat0 = err.args[0]
                                sqlstat1 = err.args[1]
                                QMessageBox.about(self, self.tr("Warning"), self.tr("\n%2").arg(sqlstat1))

                    print limit

                else:
                    print "newlogic"
                    sqlstat1 = 'You may run out of memory , but its worth a shot!! Current Memory avaliable in GB'
                    sqlstat2 = psutil.virtual_memory().free / float(1000000000)
                    sqlstat3 = str(round(sqlstat2, 2))
                    QMessageBox.about(self, self.tr("Warning"), self.tr("%1\n%2").arg(sqlstat1).arg(sqlstat3))
                    targetnolimitflag = 'ON'

            else:
                sqltarget = ('SELECT %s from %s' % (self.sourc1co2, self.targetabnam))
                try:
                    dfstable2 = (pd.read_sql(sqltarget, connx))
                    print("dfstable2 dimension :{}".format(dfstable2.shape))
                except pyodbc.Error as err:
                    sqlstat0 = err.args[0]
                    sqlstat1 = err.args[1]
                    QMessageBox.about(self, self.tr("Warning"), self.tr("\n%2").arg(sqlstat1))

            while self.completed < 100:
                self.completed += 0.0001
                self.be4bottom12.progress.setValue(self.completed)


            if targetnolimitflag == 'ON' and (sourcenolimitflag == 'ON'):
                print "Starts the looperlogic"
                if self.lcheckvalue == 'NOHADOOP':
                    cnxn = pyodbc.connect('DSN=%s;UID=%s;PWD=%s' % (self.fs2, fs0, fs1))
                    connx = pyodbc.connect('DSN=%s;UID=%s;PWD=%s' % (self.fs3, fs0, fs1))

                else:
                    cnxn = pyodbc.connect('DSN=%s;UID=%s;PWD=%s' % (self.fs2, fs0, fs1), autocommit=True, ansi=True,
                                          trusted_connection='yes')
                    connx = pyodbc.connect('DSN=%s;UID=%s;PWD=%s' % (self.fs3, fs0, fs1), autocommit=True, ansi=True,
                                           trusted_connection='yes')

                chunk_size = 100000
                qchunk_size = 100000
                nchunk_size = 100000
                chunk_minu = 99999
                offset = 1
                newnonmatching = pd.DataFrame()
                dummymatching = pd.DataFrame()
                revnewnonmatching = pd.DataFrame()

                totalcount = dfs1.iloc[0, 0]

                while (offset <= totalcount):
                    # sql = "SELECT * FROM %s limit %d offset %d"  % (sourcetabnam,chunk_size,offset) self.targetabname
                    if self.lcheckvalue == 'NOHADOOP':
                        sql1 = "SELECT %s  FROM (SELECT ROW_NUMBER() OVER(ORDER BY 1) AS rn, emp.*  FROM %s as emp)  WHERE rn BETWEEN %d AND %d" % (
                        self.sourc1col, self.sourcetabnam, offset, nchunk_size)
                    else:
                        sql1 = ('SELECT %s from %s limit %d offset %d' % (
                        self.sourc1col, self.sourcetabnam, chunk_size, offset))
                    if self.rcheckvalue == 'NOHADOOP':
                        sql2 = "SELECT %s  FROM (SELECT ROW_NUMBER() OVER(ORDER BY 1) AS rn, emp.*  FROM %s as emp)  WHERE rn BETWEEN %d AND %d" % (
                        self.sourc1co2, self.targetabnam, offset, nchunk_size)
                    else:
                        sql2 = (
                        'SELECT %s from %s limit %d offset %d' % (self.sourc1co2, self.targetabnam, chunk_size, offset))
                    print "i am in loop"

                    dfstable1 = (pd.read_sql(sql1, cnxn))
                    dfstable2 = (pd.read_sql(sql2, connx))

                    if offset > chunk_minu:
                        dummymatching = revnewnonmatching.drop('_merge', axis=1)
                        dfstable2 = dfstable2.append(dummymatching)

                    merged = dfstable1.merge(dfstable2, on=self.mergecolkey, how='left', indicator=True)
                    nonmatching = pd.DataFrame(merged[merged['_merge'] == 'left_only'])
                    revmerged = dfstable2.merge(dfstable1, on=self.mergecolkey, how='left', indicator=True)
                    revnonmatching = pd.DataFrame(revmerged[revmerged['_merge'] == 'left_only'])

                    newnonmatching = newnonmatching.append(nonmatching).drop_duplicates(self.mergecolkey)
                    revnewnonmatching = revnewnonmatching.append(revnonmatching).drop_duplicates(self.mergecolkey)


                    sqlstat1 = 'You are running low on Memory ! Current Memory avaliable in GB'
                    sqlstat2 = psutil.virtual_memory().free / float(1000000000)
                    sqlstat3 = str(round(sqlstat2, 2))
                    if sqlstat2 <= 2.0:
                        QMessageBox.about(self, self.tr("Warning"), self.tr("%1\n%2").arg(sqlstat1).arg(sqlstat3))

                    del dfstable1
                    del dfstable2
                    del merged
                    offset += chunk_size
                    if self.lcheckvalue == 'NOHADOOP':
                        nchunk_size += qchunk_size



                nonmatchdet = newnonmatching.shape
                self.bottom.printxt.setText("Non Matching  dimension :{}".format(nonmatchdet[0]))
                self.bottom.printxt3.setText("Total Number of Rows in Source :{}".format(dfs1.iloc[0, 0]))
                self.bottom.printxt4.setText("Total Number of Rows in Target :{}".format(dfs2.iloc[0, 0]))

                w = QWidget()
                result = QMessageBox.question(w, 'Message', "Do you like to save diff?",
                                              QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
                if result == QMessageBox.Yes:
                    file_name = time.strftime("%Y%m%d%H%M%S") + 'data.csv'
                    newnonmatching.to_csv(file_name, sep=',', mode='w')
                    sqlstat1 = 'Non Matching data saved to %s \n ' % (file_name)
                    QMessageBox.about(self, self.tr("Warning"), self.tr("\n%2").arg(sqlstat1))
                else:
                    print "NO"

            else:
                print "NO looperlogic implemented "

            if (targetnolimitflag != 'ON') and (sourcenolimitflag != 'ON'):

                merged = []
                nonmatching = []
                matching = []
                merged = dfstable1.merge(dfstable2, on=self.mergecolkey, how='left', indicator=True)
                nonmatching = merged[merged['_merge'] == 'left_only']
                nonmatchdet = nonmatching.shape
                self.bottom.printxt.setText("Non Matching  dimension :{}".format(nonmatchdet[0]))
                matching = merged[merged['_merge'] == 'both']
                matchdet = (matching.shape)
                self.bottom.printxt2.setText("Matching  dimension :{}".format(matchdet[0]))
                totalsrc = (dfstable1.shape)
                totaltar = (dfstable2.shape)

                self.bottom.printxt3.setText("Total Number of Rows in Source :{}".format(totalsrc[0]))
                self.bottom.printxt4.setText("Total Number of Rows in Target  :{}".format(totaltar[0]))
                w = QWidget()
                result = QMessageBox.question(w, 'Message', "Do you like to save diff?",
                                              QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
                if result == QMessageBox.Yes:
                    file_name = time.strftime("%Y%m%d%H%M%S") + 'data.csv'
                    nonmatching.to_csv(file_name, sep=',', mode='w')
                    sqlstat1 = 'Non Matching data saved to %s \n ' % (file_name)
                    QMessageBox.about(self, self.tr("Warning"), self.tr("\n%2").arg(sqlstat1))
                else:
                    print "NO"
            else:
                sqlmsg = 'One of the source or target is empty, Not Advisable to compare'
                QMessageBox.about(self, self.tr("Warning"), self.tr("\n%2").arg(sqlstat1))

            # write for the bottomright
            if targetnolimitflag == 'ON' and (sourcenolimitflag == 'ON'):
                rect = (dfs1.iloc[0, 0])
                rect1 = (dfs2.iloc[0, 0])
                rect2 = (nonmatchdet[0])
                rect3 = (int(rect) - int(rect2))
            else:
                rect = (totalsrc[0])
                rect1 = (totaltar[0])
                rect2 = (nonmatchdet[0])
                rect3 = (matchdet[0])


            self.update_figure(rect, rect1, rect2, rect3)



        else:
            sqlstat1 = 'Choose All the options,  Cant be empty'
            QMessageBox.warning(self, self.tr("Warning"), self.tr("\n%2").arg(sqlstat1))


    def close_application(self):
        choice = QtGui.QMessageBox.question(self, 'Leaving!',
                                            "Are you to sure close?",
                                            QtGui.QMessageBox.Yes | QtGui.QMessageBox.No)
        if choice == QtGui.QMessageBox.Yes:
            print("closeing Hadoop compare!!!!")
            sys.exit()
        else:
            pass

    def transpose(self):
        sqlstat1 = "This function will be avalibe in future version"
        QMessageBox.warning(self, self.tr("Warning"), self.tr("\n%2").arg(sqlstat1))


    def check(self):
        if str(self.password.text()) != '':

            return self.username, self.password
        else:
            sqlstat1 = "Password or Username can't be empty"
            QMessageBox.warning(self, self.tr("Warning"), self.tr("\n%2").arg(sqlstat1))

    def reject(self):
        if str(self.password.text()) != '':

            return self.username, self.password
        else:
            sqlstat1 = "Password or Username can't be empty"
            QMessageBox.warning(self, self.tr("Warning"), self.tr("\n%2").arg(sqlstat1))







class GraphView(QtGui.QWidget):
    def __init__(self, name, title, graph_title, parent=None):
        super(GraphView, self).__init__(parent)

        self.name = name
        self.graph_title = graph_title

        self.dpi = 70
        self.fig = Figure((3.0, 2.0), dpi=self.dpi, facecolor=(1, 1, 1), edgecolor=(0, 0, 0))
        self.axes = self.fig.add_subplot(111)
        self.canvas = FigureCanvas(self.fig)
        self.canvas.setParent(self)

        self.Title = QtGui.QLabel(self)
        self.Title.setText(title)

        self.layout = QtGui.QVBoxLayout()
        self.layout.addWidget(self.Title)
        self.layout.addWidget(self.canvas)
        self.layout.setStretchFactor(self.canvas, 1)
        self.setLayout(self.layout)
        self.canvas.show()

    def update_graph(self, df, **kwargs):
        self.axes.clear()
        df.plot.bar(ax=self.axes, **kwargs)

        self.canvas.draw()




if __name__ == '__main__':
    # global gui
    app = QtGui.QApplication([])
    ex = MainWindow()

    ex.show()
    sys.exit(app.exec_())
