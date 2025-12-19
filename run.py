"""Run the orchestrator with CLI args.

Usage example (PowerShell):
    python run.py inbound GNC 01IBGNCInboundFile
    python run.py outbound GNC 02GNCOutboundFile
"""
import sys
import logging
from common.orchestrator import Orchestrator
from common.error_handler import ErrorLogger


def setup_error_handling():
    """Setup centralized error handling."""
    error_logger = ErrorLogger()
    return error_logger


def main(argv):
    """Main entry point with error handling.
    
    Args:
        argv: Command line arguments
        
    Returns:
        int: Exit code (0 for success, non-zero for error)
    """
    error_logger = setup_error_handling()
    logger = error_logger.logger
    
    try:
        if len(argv) < 4:
            error_msg = 'Usage: python run.py <inbound|outbound> <partner> <flow_name>'
            logger.error(error_msg)
            print(error_msg)
            return 2
        
        _, direction, partner, flow_name = argv[:4]
        
        logger.info(f"Starting application: direction={direction}, partner={partner}, flow_name={flow_name}")
        
        orch = Orchestrator()
        orch.run(direction, partner, flow_name)
        
        logger.info("Application completed successfully")
        
        # Send summary if there were any warnings
        summary = error_logger.get_summary()
        if summary['warning_count'] > 0:
            logger.info(f"Warnings logged: {summary['warning_count']}")
        
        return 0
    
    except Exception as e:
        error_logger.log_error(
            f"Fatal application error: {str(e)}",
            context={'args': argv},
            exc_info=True
        )
        print(f"Fatal error: {e}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main(sys.argv))
