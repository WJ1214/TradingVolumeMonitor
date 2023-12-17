import sys
from PyQt5.QtWidgets import QApplication, QWidget, QVBoxLayout, QPushButton, QLineEdit, QDesktopWidget

class MyApp(QWidget):
  def __init__(self):
    super().__init__()

    # 设置窗口标题
    self.setWindowTitle('test')

    # 设置窗口大小
    self.resize(500, 500)
    self.init_ui()

  def init_ui(self):
    self.resize(500, 500)
    #获得主窗口所在的框架
    qr = self.frameGeometry()
    #获取显示器的分辨率，然后得到屏幕中间点的位置。
    cp = QDesktopWidget().availableGeometry().center()
    #然后把主窗口框架的中心点放置到屏幕的中心位置
    qr.moveCenter(cp)
    self.move(qr.topLeft())
    self.setWindowTitle('Center')
    input_box = QLineEdit()
    button = QPushButton('确定')
    button.resize(button.sizeHint())
    # 创建一个垂直布局
    layout = QVBoxLayout()

    # 将标签添加到布局中
    layout.addWidget(button)
    layout.addWidget(input_box)

    # 将布局设置为主窗口的中心布局
    self.setLayout(layout) 
    self.show()

if __name__ == '__main__':
   app = QApplication(sys.argv)
   ex = MyApp()
   ex.show()
   sys.exit(app.exec_())