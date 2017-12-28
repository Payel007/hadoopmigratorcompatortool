"""
	Autor : Payel Das
	Description : PyQt4 custom unload for all the DataSources including Hadoop.
	Created : Mar 10, 2017
	
"""

from PyQt4 import QtGui, QtCore


class CheckComboBox(QtGui.QComboBox):
	""" Class definition for the custom QComboBox that can select multiple
		options using check-box.
	"""
	def __init__(self, parent=None):
		""" Initialization for CheckComboBox class.


		"""
		super(CheckComboBox, self).__init__(parent)

		self._model = CheckComboModel(self)
		self._lineEdit = QtGui.QLineEdit()
		self._listView = self.view()
		self._separator = ', '
		self._defaultText = 'Nothing Selected'
		self._selectAllText = 'All Selected'
		self._maxTextLength = 42
		self._checkedItems = []

		self.setModel(self._model)
		self.setLineEdit(self._lineEdit)
		self.setInsertPolicy(QtGui.QComboBox.NoInsert)
		self._lineEdit.setReadOnly(True)

		self._lineEdit.installEventFilter(self)
		self._listView.installEventFilter(self)
		self._listView.window().installEventFilter(self)
		self._listView.viewport().installEventFilter(self)
		self.installEventFilter(self)

		self.connect(self,
					QtCore.SIGNAL('activated(int)'),
					self.toggleCheckState)
		self.connect(self._model,
					QtCore.SIGNAL('checkStateChanged()'),
					self.updateCheckedItems)
		self.connect(self._model,
					QtCore.SIGNAL('rowsInserted(const QModelIndex&,int,int)'),
					self.updateCheckedItems)
		self.connect(self._model,
					QtCore.SIGNAL('rowsRemoved(const QModelIndex&,int,int)'),
					self.updateCheckedItems)
		self.connect(self,
					QtCore.SIGNAL('clearAll()'),
					self.clearAllEvent)
		self.connect(self,
					QtCore.SIGNAL('checkAll()'),
					self.checkAllEvent)

	def defaultText(self):
		""" Function to get the default text of ComboBox.


		"""
		return self._defaultText

	def setDefaultText(self, text):
		""" Function to set the default text of ComboBox.


		"""
		self._defaultText = text
		self.updateCheckedItems()

	def selectAllText(self):
		""" Function to get value of when all the items selected.

.
		"""
		return self._selectAllText

	def setSelectAllText(self, text):
		""" Function to set the value to display when all the items selected.

		"""
		self._selectAllText = text
		self.updateCheckedItems()

	def itemCheckState(self, index):
		""" Function to get the check state of the given item.


		"""
		if self._model.itemData(index, QtCore.Qt.CheckStateRole).toInt():
			return QtCore.Qt.Checked
			QtCore.Qt.UnChecked

	def setItemCheckState(self, index, state):
		""" Function to set the check state of the given item.

		"""
		checkState = QtCore.Qt.Checked if state else QtCore.Qt.Unchecked
		self._model.setItemData(index,
								checkState,
								QtCore.Qt.CheckStateRole)

	def separator(self):
		""" Function to get the separator text for the ComboBox.

			Returns:
				(str) return combobox's separator
		"""
		return self._separator

	def setSeparator(self, separator):
		""" Function to get the separator text for the ComboBox.


		"""
		self._separator = separator
		self.updateCheckedItems()

	def maxTextLength(self):
		""" Function to get the maximum length that display on the ComboBox.


		"""
		return self._maxTextLength

	def setMaxTextLength(self, length):
		""" Function to set the maximum length that display on the ComboBox.


		"""
		self._maxTextLength = length
		self.updateCheckedItems()

	def checkedItems(self):
		""" Function to get the checked items label as list.


		"""
		itemList = QtCore.QStringList()
		if self._model:
			modelIndex = self._model.index(0,
											self.modelColumn(),
											self.rootModelIndex())
			modelIndexList = self._model.match(modelIndex,
												QtCore.Qt.CheckStateRole,
												QtCore.Qt.Checked,
												-1,
												QtCore.Qt.MatchExactly)
			for mIndex in modelIndexList:
				itemList << mIndex.data().toString()
		return itemList

	def setCheckedItems(self, items):
		""" Function to set the checked state for the given items.


		"""
		for item in items:
			index = self.Findtext(item)
			state = QtCore.Qt.Unchecked if index == -1 else QtCore.Qt.Checked
			self.setItemCheckState(index, state)

	def checkAll(self, check=False):
		""" Function to set all the items as checked or unchecked based on
			the argument.


		"""
		itemList = QtCore.QStringList()
		searchState = QtCore.Qt.Checked
		assignState = QtCore.Qt.Unchecked
		if check:
			searchState = QtCore.Qt.Unchecked
			assignState = QtCore.Qt.Checked
		if self._model:
			modelIndex = self._model.index(0,
											self.modelColumn(),
											self.rootModelIndex())
			modelIndexList = self._model.match(modelIndex,
											QtCore.Qt.CheckStateRole,
											searchState,
											-1,
											QtCore.Qt.MatchExactly)
			for mIndex in modelIndexList:
				self.setItemData(mIndex.row(),
								assignState,
								QtCore.Qt.CheckStateRole)
		return itemList

	def trimDisplayText(self, text):
		""" Function to trim the display text base on the max length property.

		"""
		textLength = self._maxTextLength - 3
		if (self._maxTextLength and
			self._maxTextLength > 3 and
			len(text) > textLength):
			text = '%s...' % text[:textLength]
		return text

	def setEditText(self, text):
		""" Override the setEditText function.


		"""
		super(CheckComboBox, self).setEditText(self.trimDisplayText(text))

	def reloadPopup(self):
		""" Function to reload the popup of the Combobox.
		"""
		currentIndex = self._listView.currentIndex()
		scrollValue = self._listView.verticalScrollBar().value()
		self.showPopup()
		self._listView.setCurrentIndex(currentIndex)
		self._listView.verticalScrollBar().setValue(scrollValue)

	def keyPressEvent(self, event):
		""" Function to override key press event.


		"""
		if event.key() in (QtCore.Qt.Key_Return, QtCore.Qt.Key_Enter):
			pass

	def eventFilter(self, obj, event):
		""" Function to filter out the unwanted events and add new
			functionalities for it.


		"""
		eventType = event.type()
		if eventType == QtCore.QEvent.KeyPress:
			return False
		if eventType == QtCore.QEvent.KeyRelease:
			eventKey = event.key()
			if (obj in (self, self._listView, self._lineEdit)
				and eventKey in (QtCore.Qt.Key_Up, QtCore.Qt.Key_Down)):
				self.reloadPopup()
				return False
			elif obj == self._listView and eventKey == QtCore.Qt.Key_Space:
				self.reloadPopup()
				return True
			elif obj == self._listView and eventKey in (QtCore.Qt.Key_Enter,
														QtCore.Qt.Key_Return,
														QtCore.Qt.Key_Escape,
														QtCore.Qt.Key_Tab):
				self.hidePopup()
				return True
		if eventType == QtCore.QEvent.MouseButtonPress:
			return False
		if eventType == QtCore.QEvent.MouseButtonRelease:
			eventButton = event.button()
			if eventButton == QtCore.Qt.RightButton:
				return False
			if obj == self._listView.viewport():
				self.emit(QtCore.SIGNAL('activated(int)'),
							self._listView.currentIndex().row())
				self.reloadPopup()
				return True
			if obj == self._lineEdit:
				self.showPopup()
			elif obj not in (self._listView, self._listView.window()):
				self.hidePopup()
		return False

	def updateCheckedItems(self, index=None, start=None, end=None):
		""" Slot to update the checkedItems when he dataChanged or
			checkStateChanged Signal emits.

		"""
		itemList = self.checkedItems()
		if not len(itemList):
			self.setEditText(self._defaultText)
		elif len(itemList) == self._model.rowCount():
			self.setEditText(self._selectAllText)
			pass
		else:
			self.setEditText(itemList.join(self._separator))
		self.emit(QtCore.SIGNAL('checkedItemsChanged(PyQt_PyObject)'), itemList)

	def toggleCheckState(self, index):
		""" Slot to update the check state of the item in the given index,
			Calls when the activated signal emits.

		"""
		value = self.itemData(index, QtCore.Qt.CheckStateRole)
		if value.toInt()[0]:
			state = QtCore.Qt.Unchecked
		else:
			state = QtCore.Qt.Checked
		self.setItemData(index, state, QtCore.Qt.CheckStateRole)

	def clearAllEvent(self):
		""" Event for clear all check from the list view.
		"""
		self.checkAll(False)

	def checkAllEvent(self):
		""" Event for check all from the list view.
		"""
		self.checkAll(True)

	def contextMenuEvent(self, event):
		""" Event for create context menu.

		"""
		signalMap = {}
		contextMenu = QtGui.QMenu(self)

		clearAllAction = contextMenu.addAction("Clear All")
		signalMap[clearAllAction] = "clearAll()"
		checkAllAction = contextMenu.addAction("Check All")
		signalMap[checkAllAction] = "checkAll()"

		contextMenuAction = contextMenu.exec_(QtGui.QCursor.pos())
		if contextMenuAction and contextMenuAction in signalMap:
			self.emit(QtCore.SIGNAL(signalMap[contextMenuAction]))


class CheckComboModel(QtGui.QStandardItemModel):
	""" Class definition for the custom QComboBox that can select multiple
		options using check-box.
	"""
	def __init__(self, parent=None):
		""" Initialization for CheckComboModel class.


		"""
		super(CheckComboModel, self).__init__(parent)

	def flags(self, index):
		""" Overwritten function to return the wanted roles for the
			given index.

		"""
		return QtCore.Qt.ItemIsUserCheckable | \
				QtCore.Qt.ItemIsSelectable | \
				QtCore.Qt.ItemIsEnabled | \
				QtCore.Qt.ItemIsEditable

	def data(self, index, role):
		""" Overwritten function to return value based on the role and index.


		"""
		value = super(CheckComboModel, self).data(index, role)
		if index.isValid() and role == QtCore.Qt.CheckStateRole:
			if not value.isValid():
				return QtCore.Qt.Unchecked
			else:
				return value
		return value

	def setData(self, index, value, role):
		""" Overwritten function to return set value based on the
			role and index.


		"""
		result = super(CheckComboModel, self).setData(index, value, role)

		if result and role == QtCore.Qt.CheckStateRole:
			self.emit(QtCore.SIGNAL('dataChanged(PyQt_PyObject, PyQt_PyObject)'),
									index,
									index)
			self.emit(QtCore.SIGNAL('checkStateChanged()'))
		return True
