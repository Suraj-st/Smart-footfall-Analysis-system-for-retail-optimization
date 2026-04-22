import sys
import subprocess
import webbrowser
import cv2
import socket
import json

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout,
    QPushButton, QHBoxLayout, QLabel, QFrame
)
from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtGui import QImage, QPixmap

from confluent_kafka import Consumer


# 🔷 Card UI
class Card(QFrame):
    def __init__(self, title):
        super().__init__()
        self.setStyleSheet("""
            QFrame {
                background-color: #1e1e2f;
                border-radius: 10px;
                padding: 10px;
            }
        """)
        layout = QVBoxLayout()
        self.setLayout(layout)

        label = QLabel(title)
        label.setStyleSheet("color: white; font-size: 14px; font-weight: bold;")
        layout.addWidget(label)

        self.inner_layout = QVBoxLayout()
        layout.addLayout(self.inner_layout)


# 🔷 Main Window
class MyWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Footfall Analysis System")
        self.resize(1000, 750)
        self.setStyleSheet("background-color: #121212;")

        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout()
        central.setLayout(main_layout)

        # 🔷 Title
        title = QLabel("Footfall Analysis Dashboard")
        title.setAlignment(Qt.AlignCenter)
        title.setStyleSheet("color: white; font-size: 22px; font-weight: bold;")
        main_layout.addWidget(title)

        # 🔷 Status
        self.status_label = QLabel("System Status: STOPPED")
        self.status_label.setAlignment(Qt.AlignCenter)
        self.status_label.setStyleSheet("color: red;")
        main_layout.addWidget(self.status_label)

        # 🔷 Kafka Status
        self.kafka_status = QLabel("Kafka: DISCONNECTED")
        self.kafka_status.setAlignment(Qt.AlignCenter)
        self.kafka_status.setStyleSheet("color: red;")
        main_layout.addWidget(self.kafka_status)

        # 🔷 Zone Counters
        zone_layout = QHBoxLayout()

        # Entrance
        entrance_layout = QVBoxLayout()
        entrance_title = QLabel("Entrance Zone")
        entrance_title.setStyleSheet("color: lightblue; font-weight: bold;")

        self.in_ent = QLabel("IN: 0")
        self.out_ent = QLabel("OUT: 0")
        self.dwell_ent = QLabel("Dwell: 0s")

        for lbl in [self.in_ent, self.out_ent, self.dwell_ent]:
            lbl.setStyleSheet("color: white; font-size: 14px;")
            entrance_layout.addWidget(lbl)

        entrance_layout.insertWidget(0, entrance_title)

        # Shelf
        shelf_layout = QVBoxLayout()
        shelf_title = QLabel("Shelf Zone")
        shelf_title.setStyleSheet("color: orange; font-weight: bold;")

        self.in_shelf = QLabel("IN: 0")
        self.out_shelf = QLabel("OUT: 0")
        self.dwell_shelf = QLabel("Dwell: 0s")

        for lbl in [self.in_shelf, self.out_shelf, self.dwell_shelf]:
            lbl.setStyleSheet("color: white; font-size: 14px;")
            shelf_layout.addWidget(lbl)

        shelf_layout.insertWidget(0, shelf_title)

        zone_layout.addLayout(entrance_layout)
        zone_layout.addLayout(shelf_layout)

        main_layout.addLayout(zone_layout)

        # 🔷 Video Feeds
        video_layout = QHBoxLayout()

        self.video_label1 = QLabel()
        self.video_label2 = QLabel()

        for v in [self.video_label1, self.video_label2]:
            v.setFixedSize(450, 250)
            v.setStyleSheet("background-color: black;")

        video_layout.addWidget(self.video_label1)
        video_layout.addWidget(self.video_label2)
        main_layout.addLayout(video_layout)

        # 🔷 Cards
        row = QHBoxLayout()

        system_card = Card("System Control")
        self.start_system_btn = QPushButton("Start System")
        self.stop_system_btn = QPushButton("Stop System")
        system_card.inner_layout.addWidget(self.start_system_btn)
        system_card.inner_layout.addWidget(self.stop_system_btn)

        entrance_card = Card("Entrance Camera")
        self.start_ent = QPushButton("Start Entrance")
        self.stop_ent = QPushButton("Stop Entrance")
        entrance_card.inner_layout.addWidget(self.start_ent)
        entrance_card.inner_layout.addWidget(self.stop_ent)

        shelf_card = Card("Shelf Area")
        self.start_shelf = QPushButton("Start Shelf")
        self.stop_shelf = QPushButton("Stop Shelf")
        shelf_card.inner_layout.addWidget(self.start_shelf)
        shelf_card.inner_layout.addWidget(self.stop_shelf)

        dash_card = Card("Dashboards")
        self.powerbi_btn = QPushButton("Open Power BI")
        self.grafana_btn = QPushButton("Open Grafana")
        dash_card.inner_layout.addWidget(self.powerbi_btn)
        dash_card.inner_layout.addWidget(self.grafana_btn)

        row.addWidget(system_card)
        row.addWidget(entrance_card)
        row.addWidget(shelf_card)
        row.addWidget(dash_card)

        main_layout.addLayout(row)

        # 🔷 Button Override (FIX)
        for btn in self.findChildren(QPushButton):
            btn.setStyleSheet("""
                QPushButton {
                    background-color: #2d89ef;
                    color: white;
                    font-size: 13px;
                    font-weight: bold;
                    padding: 10px;
                    border-radius: 6px;
                    border: none;
                }
                QPushButton:hover {
                    background-color: #1b5fa7;
                }
            """)

        # 🔷 Connections
        self.start_system_btn.clicked.connect(self.start_system)
        self.stop_system_btn.clicked.connect(self.stop_system)
        self.start_ent.clicked.connect(self.start_script1)
        self.stop_ent.clicked.connect(self.stop_script1)
        self.start_shelf.clicked.connect(self.start_script2)
        self.stop_shelf.clicked.connect(self.stop_script2)
        self.powerbi_btn.clicked.connect(self.open_powerbi)
        self.grafana_btn.clicked.connect(self.open_grafana)
        
        # 🔷 Processes
        self.process1 = None
        self.process2 = None
        self.process3 = None

        # 🔷 Video
        self.cap1 = cv2.VideoCapture("ed3.mp4")
        self.cap2 = cv2.VideoCapture("ed3.mp4")

        self.timer = QTimer()
        self.timer.timeout.connect(self.update_frame)
        self.timer.start(30)

        # 🔷 Kafka
        self.kafka_timer = QTimer()
        self.kafka_timer.timeout.connect(self.check_kafka)
        self.kafka_timer.start(3000)

        self.init_kafka()

    # 🔷 Kafka Setup
    def init_kafka(self):
        self.consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'ui-group',
            'auto.offset.reset': 'latest'
        })
        self.consumer.subscribe(['footfall_topic'])

        self.kafka_poll_timer = QTimer()
        self.kafka_poll_timer.timeout.connect(self.poll_kafka)
        self.kafka_poll_timer.start(1000)

    # 🔷 Kafka Poll
    def poll_kafka(self):
        msg = self.consumer.poll(0.1)

        if msg is None or msg.error():
            return

        try:
            data = json.loads(msg.value().decode('utf-8'))
            zone = data.get("zone", "")

            if zone == "":
                self.in_ent.setText(f"IN: {data.get('IN', 0)}")
                self.out_ent.setText(f"OUT: {data.get('OUT', 0)}")
                self.dwell_ent.setText(f"Dwell: {data.get('actual_time', 0)}s")

            elif zone == "shelf_area":
                self.in_shelf.setText(f"IN: {data.get('IN', 0)}")
                self.out_shelf.setText(f"OUT: {data.get('OUT', 0)}")
                self.dwell_shelf.setText(f"Dwell: {data.get('actual_time', 0)}s")

        except Exception as e:
            print("Kafka error:", e)

    # 🔷 Video Update
    def update_frame(self):
        ret1, f1 = self.cap1.read()
        if not ret1:
            self.cap1.set(cv2.CAP_PROP_POS_FRAMES, 0)
        else:
            f1 = cv2.cvtColor(f1, cv2.COLOR_BGR2RGB)
            h, w, ch = f1.shape
            self.video_label1.setPixmap(QPixmap.fromImage(QImage(f1.data, w, h, ch * w, QImage.Format_RGB888)))

        ret2, f2 = self.cap2.read()
        if not ret2:
            self.cap2.set(cv2.CAP_PROP_POS_FRAMES, 0)
        else:
            f2 = cv2.cvtColor(f2, cv2.COLOR_BGR2RGB)
            h, w, ch = f2.shape
            self.video_label2.setPixmap(QPixmap.fromImage(QImage(f2.data, w, h, ch * w, QImage.Format_RGB888)))

    # 🔷 Kafka Status
    def check_kafka(self):
        try:
            socket.create_connection(("localhost", 9092), timeout=1)
            self.kafka_status.setText("Kafka: CONNECTED")
            self.kafka_status.setStyleSheet("color: lightgreen;")
        except:
            self.kafka_status.setText("Kafka: DISCONNECTED")
            self.kafka_status.setStyleSheet("color: red;")

    # 🔷 Controls
    def start_system(self):
        self.status_label.setText("System Status: RUNNING")
        self.status_label.setStyleSheet("color: lightgreen;")
        self.process3 = subprocess.Popen([r"myenv\Scripts\python", "kafka_to_postgres_consumer.py"])

    def stop_system(self):
        self.status_label.setText("System Status: STOPPED")
        self.status_label.setStyleSheet("color: red;")
        if self.process3:
            self.process3.terminate()

    def start_script1(self):
        self.process1 = subprocess.Popen([r"myenv\Scripts\python", "counting_time_diff_sec_db_kafka.py"])

    def stop_script1(self):
        if self.process1:
            self.process1.terminate()

    def start_script2(self):
        self.process2 = subprocess.Popen([r"myenv\Scripts\python", "counting_dbl_side_diff_sec_db_kafka.py"])

    def stop_script2(self):
        if self.process2:
            self.process2.terminate()

    def open_powerbi(self):
        webbrowser.open("https://app.powerbi.com/groups/83355bd3-2c2d-49c9-a235-7f30804266aa/reports/068c93cc-1fd7-47e0-aada-0051a8f6d202/ReportSection15dbfb33205b066302e7?experience=power-bi")

    def open_grafana(self):
        webbrowser.open("http://localhost:3000/d/adjqmwc/footfall-analysis-dashboard?orgId=1&from=now-6h&to=now&timezone=browser")

    def closeEvent(self, event):
        if self.cap1.isOpened():
            self.cap1.release()
        if self.cap2.isOpened():
            self.cap2.release()
        self.consumer.close()
        event.accept()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MyWindow()
    window.show()
    sys.exit(app.exec_())