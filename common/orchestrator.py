import os
import logging

from .config_loader import ConfigLoader
from .local_sftp import LocalSFTP
from .db_client import DBClient
from .email_notifier import EmailNotifier
from .oauth_client import OAuthClient
from .error_handler import ErrorLogger

from .inbound_processor import InboundProcessor
from .outbound_processor import OutboundProcessor


class Orchestrator:
   
    def __init__(self, config_path=None):
        self.cfg = ConfigLoader(config_path)
        
        # Initialize error logger with centralized logs folder
        log_folder = self.cfg.get_global('log_folder', os.path.join(os.path.dirname(self.cfg.path), '..', 'logs'))
        self.error_logger = ErrorLogger(log_folder)
        self.logger = self.error_logger.logger
        
        # Local SFTP helper uses repo root dir for local operations
        self.local_sftp = LocalSFTP(os.path.abspath(os.path.dirname(self.cfg.path)))
        self.db = DBClient(self.cfg.get_db())
        try:
            self.db.connect()
            self.logger.info("Database connection established")
        except Exception as e:
            # DB optional — log error but don't fail (they'll reappear if insert attempted)
            self.error_logger.log_error(
                "Database connection failed",
                context={'error': str(e)},
                exc_info=True
            )
        
        # Initialize email notifier with error handling
        self.email = EmailNotifier(self.cfg.get_email(), self.error_logger)
        self.oauth = OAuthClient(self.cfg.get_auth())

        # processors
        self.inbound_processor = InboundProcessor(self.cfg, self.local_sftp, self.db, self.email, self.oauth)
        self.outbound_processor = OutboundProcessor(self.cfg, self.local_sftp, self.db, self.email, self.oauth)

    def run(self, direction: str, partner: str, flow_name: str):
        
        try:
            direction = direction.lower()
            self.logger.info(f"Starting {direction} flow for partner={partner}, flow_name={flow_name}")
            
            if direction == 'inbound':
                self.inbound_processor.process(partner, flow_name)
            elif direction == 'outbound':
                self.outbound_processor.process(partner, flow_name)
            else:
                raise ValueError('direction must be inbound or outbound')
            
            self.logger.info(f"Completed {direction} flow for {partner}/{flow_name}")
            
        except Exception as e:
            self.error_logger.log_error(
                f"Orchestrator error: {str(e)}",
                context={'direction': direction, 'partner': partner, 'flow_name': flow_name},
                exc_info=True
            )
            
            # Send error summary email
            summary = self.error_logger.get_summary()
            
            
            raise

