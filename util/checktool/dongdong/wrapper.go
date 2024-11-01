package dongdong

type CommonAlarm struct {
	app   string
	gid   int
	alarm *DDAlarm
}

func NewCommonAlarm(gid int, app string) (alarm *CommonAlarm, err error) {
	alarm = new(CommonAlarm)
	alarm.gid = gid
	alarm.app = app
	alarm.alarm, err = NewGroupDDAlarm(gid, app)
	return
}

func (alarm *CommonAlarm) Alarm(key, detail string) (err error) {
	return alarm.alarm.alarmToGroup(key, detail)
}
