import tkinter as tk
from tkinter import ttk, scrolledtext, filedialog, messagebox
import socket
import json
import threading
import os
from datetime import datetime
import base64
import struct
import sys
import subprocess

class MessengerClient:
    def __init__(self):
        self.username = None
        self.current_chat = None
        self.main_socket = None  # Main socket for chat
        
        # Create main window
        self.root = tk.Tk()
        self.root.title("Messenger")
        self.root.geometry("400x600")
        self.root.configure(bg='#2b2b2b')
        
        # Create styles
        self.style = ttk.Style()
        self.style.configure('Dark.TFrame', background='#2b2b2b')
        self.style.configure('Dark.TButton', background='#404040', foreground='white')
        self.style.configure('Dark.TLabel', background='#2b2b2b', foreground='white')
        self.style.configure('Dark.TEntry', fieldbackground='white', foreground='black')
        
        # Show login window
        self.show_login_window()
        
    def create_socket(self):
        """Create a new socket connection to the server"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(('localhost', 5000))
            return sock
        except Exception as e:
            messagebox.showerror("Error", f"Could not connect to server: {e}")
            return None
            
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
            
    def send_request(self, request):
        """Send request to server and get response"""
        sock = self.create_socket()
        if not sock:
            return None
            
        try:
            self.send_json(sock, request)
            response = self.recv_json(sock)
            return response
        except Exception as e:
            messagebox.showerror("Error", f"Server communication error: {e}")
            return None
        finally:
            sock.close()
            
    def show_login_window(self):
        """Show login/register window"""
        self.clear_window()
        
        # Login frame
        login_frame = ttk.Frame(self.root, style='Dark.TFrame')
        login_frame.pack(pady=20)
        
        ttk.Label(login_frame, text="Username:", style='Dark.TLabel').pack()
        self.username_entry = ttk.Entry(login_frame, style='Dark.TEntry')
        self.username_entry.pack(pady=5)
        
        ttk.Label(login_frame, text="Password:", style='Dark.TLabel').pack()
        self.password_entry = ttk.Entry(login_frame, show="*", style='Dark.TEntry')
        self.password_entry.pack(pady=5)
        
        # Buttons
        button_frame = ttk.Frame(login_frame, style='Dark.TFrame')
        button_frame.pack(pady=10)
        
        ttk.Button(button_frame, text="Login", command=self.login).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Register", command=self.register).pack(side=tk.LEFT, padx=5)
        
    def show_contacts_window(self):
        """Show contacts window"""
        self.clear_window()
        
        # Contacts frame
        contacts_frame = ttk.Frame(self.root, style='Dark.TFrame')
        contacts_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Add contact button
        add_button = ttk.Button(contacts_frame, text="+ Add Contact", command=self.show_add_contact_dialog)
        add_button.pack(pady=5)
        
        # Contacts list
        self.contacts_listbox = tk.Listbox(contacts_frame, bg='#404040', fg='white', selectbackground='#505050')
        self.contacts_listbox.pack(fill=tk.BOTH, expand=True)
        self.contacts_listbox.bind('<Double-Button-1>', self.open_chat)
        
        # Refresh contacts
        self.refresh_contacts()
        
    def show_chat_window(self, contact):
        """Show chat window with selected contact"""
        self.clear_window()
        self.current_chat = contact
        
        # Chat frame
        chat_frame = ttk.Frame(self.root, style='Dark.TFrame')
        chat_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Back button
        back_button = ttk.Button(chat_frame, text="← Back", command=self.show_contacts_window)
        back_button.pack(anchor=tk.W, pady=5)
        
        # Chat area
        self.chat_area = scrolledtext.ScrolledText(chat_frame, bg='#404040', fg='white', wrap=tk.WORD)
        self.chat_area.pack(fill=tk.BOTH, expand=True, pady=5)
        self.chat_area.config(state=tk.DISABLED)
        
        # Message input
        input_frame = ttk.Frame(chat_frame, style='Dark.TFrame')
        input_frame.pack(fill=tk.X, pady=5)
        
        self.message_entry = ttk.Entry(input_frame, style='Dark.TEntry')
        self.message_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        
        send_button = ttk.Button(input_frame, text="Send", command=self.send_message)
        send_button.pack(side=tk.LEFT)
        
        file_button = ttk.Button(input_frame, text="📎", command=self.send_file)
        file_button.pack(side=tk.LEFT, padx=5)
        
        # Load chat history
        self.load_chat_history()
        
    def clear_window(self):
        """Clear all widgets from the window"""
        for widget in self.root.winfo_children():
            widget.destroy()
            
    def login(self):
        """Login to the server"""
        username = self.username_entry.get()
        password = self.password_entry.get()
        
        request = {
            'action': 'login',
            'username': username,
            'password': password
        }
        
        response = self.send_request(request)
        if not response:
            return
            
        if response['status'] == 'success':
            self.username = username
            # Create main socket for chat
            self.main_socket = self.create_socket()
            if self.main_socket:
                self.show_contacts_window()
                # Start message listener thread
                threading.Thread(target=self.listen_for_messages, daemon=True).start()
        else:
            messagebox.showerror("Error", response['message'])
            
    def register(self):
        """Register new user"""
        username = self.username_entry.get()
        password = self.password_entry.get()
        
        request = {
            'action': 'register',
            'username': username,
            'password': password
        }
        
        response = self.send_request(request)
        if not response:
            return
            
        if response['status'] == 'success':
            messagebox.showinfo("Success", "Registration successful! Please login.")
        else:
            messagebox.showerror("Error", response['message'])
            
    def show_add_contact_dialog(self):
        """Show dialog to add new contact"""
        dialog = tk.Toplevel(self.root)
        dialog.title("Add Contact")
        dialog.geometry("300x100")
        dialog.configure(bg='#2b2b2b')
        
        ttk.Label(dialog, text="Contact Username:", style='Dark.TLabel').pack(pady=5)
        contact_entry = ttk.Entry(dialog, style='Dark.TEntry')
        contact_entry.pack(pady=5)
        
        def add_contact():
            contact_username = contact_entry.get()
            request = {
                'action': 'add_contact',
                'username': self.username,
                'contact_username': contact_username
            }
            
            response = self.send_request(request)
            if not response:
                return
                
            if response['status'] == 'success':
                messagebox.showinfo("Success", "Contact added successfully!")
                self.refresh_contacts()
                dialog.destroy()
            else:
                messagebox.showerror("Error", response['message'])
                
        ttk.Button(dialog, text="Add", command=add_contact).pack(pady=5)
        
    def refresh_contacts(self):
        """Refresh contacts list"""
        request = {
            'action': 'get_contacts',
            'username': self.username
        }
        
        response = self.send_request(request)
        if not response:
            return
            
        if response['status'] == 'success':
            self.contacts_listbox.delete(0, tk.END)
            for contact in response['contacts']:
                self.contacts_listbox.insert(tk.END, contact)
                
    def open_chat(self, event):
        """Open chat with selected contact"""
        selection = self.contacts_listbox.curselection()
        if selection:
            contact = self.contacts_listbox.get(selection[0])
            self.show_chat_window(contact)
            
    def send_message(self):
        """Send message to current chat"""
        message = self.message_entry.get()
        if not message:
            return
            
        request = {
            'action': 'send_message',
            'sender': self.username,
            'receiver': self.current_chat,
            'content': message
        }
        
        if self.main_socket:
            try:
                self.send_json(self.main_socket, request)
                # Display sent message immediately
                self.display_message({
                    'sender': self.username,
                    'content': message,
                    'is_file': False
                })
                self.message_entry.delete(0, tk.END)
            except:
                messagebox.showerror("Error", "Lost connection to server")
                
    def send_file(self):
        """Send file to current chat"""
        file_path = filedialog.askopenfilename()
        if not file_path:
            return
        file_name = os.path.basename(file_path)
        with open(file_path, 'rb') as f:
            file_content = f.read()
        file_content_b64 = base64.b64encode(file_content).decode('utf-8')
        request = {
            'action': 'send_message',
            'sender': self.username,
            'receiver': self.current_chat,
            'content': file_name,
            'is_file': True,
            'file_path': file_name,
            'file_content': file_content_b64
        }
        if self.main_socket:
            try:
                self.send_json(self.main_socket, request)
                self.display_message({
                    'sender': self.username,
                    'content': file_name,
                    'is_file': True,
                    'file_path': file_name
                })
            except:
                messagebox.showerror("Error", "Lost connection to server")
                
    def load_chat_history(self):
        """Load chat history with current contact"""
        request = {
            'action': 'get_messages',
            'user1': self.username,
            'user2': self.current_chat
        }
        
        response = self.send_request(request)
        if not response:
            return
            
        if response['status'] == 'success':
            self.chat_area.config(state=tk.NORMAL)
            self.chat_area.delete(1.0, tk.END)
            
            for message in response['messages']:
                self.display_message(message)
                
            self.chat_area.config(state=tk.DISABLED)
            self.chat_area.see(tk.END)
            
    def display_message(self, message):
        """Display message in chat area"""
        self.chat_area.config(state=tk.NORMAL)
        
        # Configure tags for message colors
        self.chat_area.tag_configure('sender', foreground='#007bff')  # Blue for sender
        self.chat_area.tag_configure('receiver', foreground='white')  # White for receiver
        
        # Add message
        if message.get('is_file'):
            tag = 'sender' if message['sender'] == self.username else 'receiver'
            self.chat_area.insert(tk.END, f"{message['sender'] if message['sender'] != self.username else 'You'}: ", tag)
            start = self.chat_area.index(tk.END)
            self.chat_area.insert(tk.END, f"[File: {message['content']}]\n", (tag, 'file_link'))
            end = self.chat_area.index(tk.END)
            self.chat_area.tag_add('file_link', start, end)
            self.chat_area.tag_bind('file_link', '<Button-1>', lambda e, m=message: self.open_file(m))
            # Автоматически скачиваем файл, если его нет
            file_path = message['file_path']
            local_path = os.path.join(os.getcwd(), os.path.basename(file_path))
            if not os.path.exists(local_path):
                file_content_b64 = self.request_file_from_server(file_path)
                if file_content_b64:
                    with open(local_path, 'wb') as f:
                        f.write(base64.b64decode(file_content_b64))
        else:
            if message['sender'] == self.username:
                self.chat_area.insert(tk.END, f"You: {message['content']}\n", 'sender')
            else:
                self.chat_area.insert(tk.END, f"{message['sender']}: {message['content']}\n", 'receiver')
            
        self.chat_area.config(state=tk.DISABLED)
        self.chat_area.see(tk.END)  # Scroll to the latest message
        
    def open_file_crossplatform(self, path):
        if sys.platform.startswith('darwin'):
            subprocess.call(('open', path))
        elif sys.platform.startswith('win'):
            os.startfile(path)
        elif sys.platform.startswith('linux'):
            subprocess.call(('xdg-open', path))

    def open_file(self, message):
        file_path = message['file_path']
        local_path = os.path.join(os.getcwd(), os.path.basename(file_path))
        if not os.path.exists(local_path):
            file_content_b64 = self.request_file_from_server(file_path)
            if file_content_b64:
                with open(local_path, 'wb') as f:
                    f.write(base64.b64decode(file_content_b64))
            else:
                messagebox.showerror("Error", f"Could not download file {file_path}")
                return
        self.open_file_crossplatform(local_path)
        
    def request_file_from_server(self, file_path):
        request = {
            'action': 'get_file',
            'file_path': file_path
        }
        sock = self.create_socket()
        if not sock:
            return None
        try:
            self.send_json(sock, request)
            response = self.recv_json(sock)
            if response['status'] == 'success':
                return response['file_content']
            else:
                return None
        except Exception as e:
            messagebox.showerror("Error", f"File download error: {e}")
            return None
        finally:
            sock.close()
        
    def listen_for_messages(self):
        """Listen for incoming messages"""
        while True:
            try:
                if not self.main_socket:
                    break
                data = self.recv_json(self.main_socket)
                if not data:
                    break
                message = data
                if message['action'] == 'new_message':
                    # Show message if current chat is with sender or receiver
                    if self.current_chat == message['sender'] or self.current_chat == message.get('receiver'):
                        self.display_message(message)
                    else:
                        self.root.after(0, lambda: messagebox.showinfo(
                            "New Message",
                            f"New message from {message['sender']}: {message['content']}"
                        ))
            except Exception as e:
                print(f"Error in message listener: {e}")
                break
                
    def run(self):
        """Start the client"""
        self.root.mainloop()

if __name__ == '__main__':
    client = MessengerClient()
    client.run() 