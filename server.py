import socket
import threading
import sqlite3
import json
import os
from datetime import datetime
import base64
import struct

class MessengerServer:
    def __init__(self, host='0.0.0.0', port=5000):
        self.host = host
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.clients = {}  # {username: (socket, address)}
        self.initialize_database()
        
    def initialize_database(self):
        conn = sqlite3.connect('messenger.db')
        cursor = conn.cursor()
        
        #create tables
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS contacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            contact_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (contact_id) REFERENCES users (id)
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_id INTEGER,
            receiver_id INTEGER,
            content TEXT,
            file_path TEXT,
            is_file BOOLEAN DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (sender_id) REFERENCES users (id),
            FOREIGN KEY (receiver_id) REFERENCES users (id)
        )
        ''')
        
        conn.commit()
        conn.close()
        
        #make files folder
        if not os.path.exists('files'):
            os.makedirs('files')

    def start(self):
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        print(f"Server started on {self.host}:{self.port}")
        
        while True:
            client_socket, address = self.server_socket.accept()
            client_thread = threading.Thread(target=self.handle_client, args=(client_socket, address))
            client_thread.start()

    def handle_client(self, client_socket, address):
        try:
            while True:
                data = self.recv_json(client_socket)
                if not data:
                    break
                    
                request = data
                response = self.process_request(request, client_socket)
                self.send_json(client_socket, response)
                
        except Exception as e:
            print(f"Error handling client {address}: {e}")
        finally:
            #remove client when they disconnect
            for username, (socket, _) in list(self.clients.items()):
                if socket == client_socket:
                    del self.clients[username]
                    break
            client_socket.close()

    def process_request(self, request, client_socket):
        action = request.get('action')
        
        if action == 'register':
            return self.register_user(request)
        elif action == 'login':
            return self.login_user(request, client_socket)
        elif action == 'add_contact':
            return self.add_contact(request)
        elif action == 'get_contacts':
            return self.get_contacts(request)
        elif action == 'send_message':
            return self.send_message(request)
        elif action == 'get_messages':
            return self.get_messages(request)
        elif action == 'get_file':
            return self.get_file(request)
        else:
            return {'status': 'error', 'message': 'Invalid action'}

    def register_user(self, request):
        try:
            conn = sqlite3.connect('messenger.db')
            cursor = conn.cursor()
            
            username = request.get('username')
            password = request.get('password')
            
            cursor.execute('INSERT INTO users (username, password) VALUES (?, ?)',
                         (username, password))
            conn.commit()
            
            return {'status': 'success', 'message': 'Registration successful'}
        except sqlite3.IntegrityError:
            return {'status': 'error', 'message': 'Username already exists'}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
        finally:
            conn.close()

    def login_user(self, request, client_socket):
        try:
            conn = sqlite3.connect('messenger.db')
            cursor = conn.cursor()
            
            username = request.get('username')
            password = request.get('password')
            
            cursor.execute('SELECT id FROM users WHERE username = ? AND password = ?',
                         (username, password))
            user = cursor.fetchone()
            
            if user:
                #remove old socket if user was already logged in
                if username in self.clients:
                    try:
                        old_socket = self.clients[username][0]
                        old_socket.close()
                    except:
                        pass
                self.clients[username] = (client_socket, None)
                return {'status': 'success', 'message': 'Login successful'}
            else:
                return {'status': 'error', 'message': 'Invalid credentials'}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
        finally:
            conn.close()

    def add_contact(self, request):
        try:
            conn = sqlite3.connect('messenger.db')
            cursor = conn.cursor()
            
            username = request.get('username')
            contact_username = request.get('contact_username')
            
            #get user ids
            cursor.execute('SELECT id FROM users WHERE username = ?', (username,))
            user_id = cursor.fetchone()[0]
            
            cursor.execute('SELECT id FROM users WHERE username = ?', (contact_username,))
            contact_id = cursor.fetchone()[0]
            
            cursor.execute('INSERT INTO contacts (user_id, contact_id) VALUES (?, ?)',
                         (user_id, contact_id))
            conn.commit()
            
            return {'status': 'success', 'message': 'Contact added successfully'}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
        finally:
            conn.close()

    def get_contacts(self, request):
        try:
            conn = sqlite3.connect('messenger.db')
            cursor = conn.cursor()
            
            username = request.get('username')
            
            cursor.execute('''
                SELECT u.username 
                FROM users u
                JOIN contacts c ON u.id = c.contact_id
                JOIN users u2 ON c.user_id = u2.id
                WHERE u2.username = ?
            ''', (username,))
            
            contacts = [row[0] for row in cursor.fetchall()]
            return {'status': 'success', 'contacts': contacts}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
        finally:
            conn.close()

    def send_message(self, request):
        try:
            conn = sqlite3.connect('messenger.db')
            cursor = conn.cursor()
            sender = request.get('sender')
            receiver = request.get('receiver')
            content = request.get('content')
            is_file = request.get('is_file', False)
            file_path = request.get('file_path', None)
            
            #handle file upload
            if is_file and 'file_content' in request and file_path:
                file_content_b64 = request['file_content']
                file_bytes = base64.b64decode(file_content_b64)
                save_path = os.path.join('files', file_path)
                with open(save_path, 'wb') as f:
                    f.write(file_bytes)
                file_path = save_path
            
            #get user ids
            cursor.execute('SELECT id FROM users WHERE username = ?', (sender,))
            sender_id = cursor.fetchone()[0]
            
            cursor.execute('SELECT id FROM users WHERE username = ?', (receiver,))
            receiver_id = cursor.fetchone()[0]
            
            #save message
            cursor.execute('''
                INSERT INTO messages (sender_id, receiver_id, content, file_path, is_file)
                VALUES (?, ?, ?, ?, ?)
            ''', (sender_id, receiver_id, content, file_path, is_file))
            conn.commit()
            
            #notify receiver if online
            if receiver in self.clients:
                receiver_socket = self.clients[receiver][0]
                notification = {
                    'action': 'new_message',
                    'sender': sender,
                    'receiver': receiver,
                    'content': content,
                    'is_file': is_file,
                    'file_path': file_path
                }
                self.send_json(receiver_socket, notification)
            
            return {'status': 'success', 'message': 'Message sent successfully'}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
        finally:
            conn.close()

    def get_messages(self, request):
        try:
            conn = sqlite3.connect('messenger.db')
            cursor = conn.cursor()
            
            user1 = request.get('user1')
            user2 = request.get('user2')
            
            #get user ids
            cursor.execute('SELECT id FROM users WHERE username = ?', (user1,))
            user1_id = cursor.fetchone()[0]
            
            cursor.execute('SELECT id FROM users WHERE username = ?', (user2,))
            user2_id = cursor.fetchone()[0]
            
            #get messages between users
            cursor.execute('''
                SELECT u.username, m.content, m.is_file, m.file_path, m.created_at
                FROM messages m
                JOIN users u ON m.sender_id = u.id
                WHERE (m.sender_id = ? AND m.receiver_id = ?)
                   OR (m.sender_id = ? AND m.receiver_id = ?)
                ORDER BY m.created_at
            ''', (user1_id, user2_id, user2_id, user1_id))
            
            messages = []
            for row in cursor.fetchall():
                messages.append({
                    'sender': row[0],
                    'content': row[1],
                    'is_file': bool(row[2]),
                    'file_path': row[3],
                    'timestamp': row[4]
                })
            
            return {'status': 'success', 'messages': messages}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
        finally:
            conn.close()

    def get_file(self, request):
        try:
            file_path = request.get('file_path')
            if not file_path:
                return {'status': 'error', 'message': 'No file path provided'}
            
            #read file
            with open(file_path, 'rb') as f:
                file_content = f.read()
            
            #encode file
            file_content_b64 = base64.b64encode(file_content).decode('utf-8')
            
            return {
                'status': 'success',
                'file_content': file_content_b64
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    def send_json(self, sock, obj):
        data = json.dumps(obj).encode('utf-8')
        length = struct.pack('>I', len(data))
        sock.sendall(length + data)

    def recv_json(self, sock):
        raw_length = self.recvall(sock, 4)
        if not raw_length:
            return None
        length = struct.unpack('>I', raw_length)[0]
        data = self.recvall(sock, length)
        if not data:
            return None
        return json.loads(data.decode('utf-8'))

    def recvall(self, sock, n):
        data = b''
        while len(data) < n:
            packet = sock.recv(n - len(data))
            if not packet:
                return None
            data += packet
        return data

if __name__ == '__main__':
    server = MessengerServer()
    server.start() 