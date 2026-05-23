package varc

import "go.uber.org/zap"

type zapPrintfLogger struct {
	log *zap.Logger
}

func (l zapPrintfLogger) Debugf(format string, args ...any) {
	if l.log != nil {
		l.log.Sugar().Debugf(format, args...)
	}
}

func (l zapPrintfLogger) Infof(format string, args ...any) {
	if l.log != nil {
		l.log.Sugar().Infof(format, args...)
	}
}

func (l zapPrintfLogger) Warnf(format string, args ...any) {
	if l.log != nil {
		l.log.Sugar().Warnf(format, args...)
	}
}

func (l zapPrintfLogger) Errorf(format string, args ...any) {
	if l.log != nil {
		l.log.Sugar().Errorf(format, args...)
	}
}
