
import pysftp


class SftpConector:
    def __init__(self, hostname=None, port=None, username=None, password=None, private_key=None) -> None:
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.private_key = private_key

    def open_conection(self):
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        with pysftp.Connection(
                host=self.hostname,
                username=self.username,
                private_key=self.private_key,
                private_key_pass="w@ferreira5",
                port=22,
                cnopts=cnopts
        ) as sftp:
            print("Connection succesfully stablished ... ")

            directory_structure = sftp.listdir_attr()
            for attr in directory_structure:
                print(attr.filename, attr)


brl_host = 'sftran.brltrust.com.br'
brl_username = 'captalys'
brl_private_key = '/home/hc/dev/sftp/acessoBRL.pem'
brl_password = "w@ferreira5"
port = 22

sftp_conector = SftpConector(brl_host, 22, brl_username, brl_password, brl_private_key)
sftp_conector.open_conection()

