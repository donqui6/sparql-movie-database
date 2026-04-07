import customtkinter as ctk
from MongoDbConnection import MongoDbConnection

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("MongoDB app")
        self.geometry("800x600")

        self.db = MongoDbConnection()
        self.db.setUri()
        self.db.setClient()
        self.db.setDatabase()
        self.db.setCollection(keepMongoCollection=True)

        # --- Zone de requête ---
        self.query_label = ctk.CTkLabel(self, text="Requête SPARQL :")
        self.query_label.pack(pady=(20, 5))

        self.query_box = ctk.CTkTextbox(self, height=150, width=700)
        self.query_box.pack()

        # --- Bouton envoyer ---
        self.send_btn = ctk.CTkButton(self, text="Envoyer", command=self.send_query)
        self.send_btn.pack(pady=10)

        # --- Zone de résultat ---
        self.result_label = ctk.CTkLabel(self, text="Résultat :")
        self.result_label.pack()

        self.result_box = ctk.CTkTextbox(self, height=250, width=700)
        self.result_box.pack()

    def send_query(self):
        #TODO: créer la requête côté tkinter
        try:
            self.result_box.delete("1.0", "end")
            self.result_box.insert("end", "Requête envoyée avec succès ✓")
        except Exception as e:
            self.result_box.insert("end", f"Erreur : {e}")

if __name__ == "__main__":
    app = App()
    app.mainloop()