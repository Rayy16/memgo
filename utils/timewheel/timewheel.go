package timewheel

import (
	"container/list"
	"memgo/logger"
	"time"
)

// 定义一个结构体，表示任务在时间轮中的位置
type location struct {
	slot  int           // 任务所在的槽位
	etask *list.Element // 任务在槽位中的位置
}

// 定义时间轮结构体
type TimeWheel struct {
	interval time.Duration // 时间轮转动的时间间隔
	ticker   *time.Ticker  // 时间轮的定时器
	slots    []*list.List  // 时间轮的槽位

	timer             map[string]*location // 存储任务的map
	currentPos        int                  // 当前时间轮的位置
	slotNum           int                  // 时间轮的槽数
	addTaskChannel    chan task            // 添加任务的通道
	removeTaskChannel chan string          // 移除任务的通道
	stopChannel       chan bool            // 停止时间轮的通道
}

// 定义任务结构体
type task struct {
	delay  time.Duration // 任务延迟时间
	circle int           // 任务需要转动的圈数
	key    string        // 任务的键
	job    func()        // 任务的函数
}

// New函数用于创建一个新的时间轮
func New(interval time.Duration, slotNum int) *TimeWheel {
	if interval <= 0 || slotNum <= 0 {
		return nil
	}
	tw := &TimeWheel{
		interval:          interval,
		slots:             make([]*list.List, slotNum),
		timer:             make(map[string]*location),
		currentPos:        0,
		slotNum:           slotNum,
		addTaskChannel:    make(chan task),
		removeTaskChannel: make(chan string),
		stopChannel:       make(chan bool),
	}
	tw.initSlots()

	return tw
}

// 初始化时间轮的槽位
func (tw *TimeWheel) initSlots() {
	for i := 0; i < tw.slotNum; i++ {
		tw.slots[i] = list.New()
	}
}

// 启动时间轮的定时器
func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval)
	go tw.start()
}

// 停止时间轮
func (tw *TimeWheel) Stop() {
	tw.stopChannel <- true
}

// 添加任务到时间轮的任务队列
func (tw *TimeWheel) AddJob(delay time.Duration, key string, job func()) {
	if delay < 0 {
		return
	}
	tw.addTaskChannel <- task{delay: delay, key: key, job: job}
}

// 从时间轮的任务队列中移除任务
// 如果任务已完成或未找到，则不执行任何操作
func (tw *TimeWheel) RemoveJob(key string) {
	if key == "" {
		return
	}
	tw.removeTaskChannel <- key
}

// 启动时间轮
func (tw *TimeWheel) start() {
	for {
		select {
		case <-tw.ticker.C: // 等待时间轮的定时器
			tw.tickHandler() // 处理时间轮的tick
		case task := <-tw.addTaskChannel: // 等待添加任务的通道
			tw.addTask(&task) // 添加任务到时间轮的任务队列
		case key := <-tw.removeTaskChannel: // 等待移除任务的通道
			tw.removeTask(key) // 从时间轮的任务队列中移除任务
		case <-tw.stopChannel: // 等待停止时间轮的通道
			tw.ticker.Stop() // 停止时间轮的定时器
			return
		}
	}
}

// 时间轮的tick处理函数
func (tw *TimeWheel) tickHandler() {
	// 获取当前位置的槽位
	l := tw.slots[tw.currentPos]
	// 如果当前位置是最后一个槽位，则将当前位置重置为0，否则将当前位置加1
	if tw.currentPos == tw.slotNum-1 {
		tw.currentPos = 0
	} else {
		tw.currentPos++
	}
	// 扫描并运行当前槽位中的任务
	go tw.scanAndRunTask(l)
}

func (tw *TimeWheel) scanAndRunTask(l *list.List) {
	// 遍历当前槽位中的任务
	for e := l.Front(); e != nil; {
		task := e.Value.(*task)
		// 如果任务需要转动的圈数大于0，则将圈数减1，继续遍历下一个任务
		if task.circle > 0 {
			task.circle--
			e = e.Next()
			continue
		}

		// 如果任务需要转动的圈数等于0，则执行任务
		go func() {
			defer func() {
				if err := recover(); err != nil {
					logger.Error(err)
				}
			}()
			job := task.job
			job()
		}()
		// 从槽位中移除任务
		next := e.Next()
		l.Remove(e)
		// 如果任务有键，则从时间轮的任务map中移除任务
		if task.key != "" {
			delete(tw.timer, task.key)
		}
		e = next
	}
}

func (tw *TimeWheel) addTask(task *task) {
	// 获取任务在时间轮中的位置和需要转动的圈数
	pos, circle := tw.getPositionAndCircle(task.delay)
	task.circle = circle

	// 将任务添加到对应的槽位中
	e := tw.slots[pos].PushBack(task)
	loc := &location{
		slot:  pos,
		etask: e,
	}
	// 如果任务有键，则将任务添加到时间轮的任务map中
	if task.key != "" {
		_, ok := tw.timer[task.key]
		if ok {
			tw.removeTask(task.key)
		}
	}
	tw.timer[task.key] = loc
}

// getPositionAndCircle函数用于获取任务在时间轮中的位置和需要转动的圈数
func (tw *TimeWheel) getPositionAndCircle(d time.Duration) (pos int, circle int) {
	// 将任务延迟时间转换为秒数
	delaySeconds := int(d.Seconds())
	// 将时间轮转动的时间间隔转换为秒数
	intervalSeconds := int(tw.interval.Seconds())
	// 计算任务需要转动的圈数
	circle = int(delaySeconds / intervalSeconds / tw.slotNum)
	// 计算任务在时间轮中的位置
	pos = int(tw.currentPos+delaySeconds/intervalSeconds) % tw.slotNum

	return
}

// removeTask函数用于从时间轮的任务队列中移除任务
func (tw *TimeWheel) removeTask(key string) {
	pos, ok := tw.timer[key]
	if !ok {
		return
	}
	l := tw.slots[pos.slot]
	l.Remove(pos.etask)
	delete(tw.timer, key)
}
