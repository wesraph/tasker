package tasker

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/kr/pretty"

	// Import pq globally
	_ "github.com/lib/pq"
	"github.com/volatiletech/sqlboiler/boil"
	"github.com/volatiletech/sqlboiler/queries/qm"

	m "github.com/wesraph/tasker/models"
)

// Typed errors
var (
	ErrStepNotFound        = fmt.Errorf("step not found")
	ErrMissingTaskName     = fmt.Errorf("missing task name")
	ErrMissingStepName     = fmt.Errorf("missing step name")
	ErrMissingSteps        = fmt.Errorf("missing steps in task")
	ErrMissingExecFunction = fmt.Errorf("missing exec function")
	ErrReachedMaxRetry     = fmt.Errorf("reached max retry for task")
	ErrReachedEndOfTask    = fmt.Errorf("reached end of task")
	ErrNilUserTask         = fmt.Errorf("user task is nil")
)

var ctx context.Context
var dbh *sql.DB

// Step is a function to execute
type Step struct {
	Name string
	Exec func(t *Task) error
}

// Task is a group of steps
type Task struct {
	Name     string
	Steps    []Step
	UserTask *UserTask
	MaxRetry int
}

// UserTask is a user task
type UserTask struct {
	Buffer interface{}
	Args   interface{}
	*m.Task
}

// UpdateDB update the task in db
func (u UserTask) UpdateDB() error {
	var err error
	if u.Buffer != nil {
		err = u.UserBuffer.Marshal(u.Buffer)
		if err != nil {
			return err
		}
	}
	_, err = u.Update(ctx, dbh, boil.Infer())
	return err
}

// Scheduler is a group of tasks
type Scheduler struct {
	Tasks []Task
}

// Init the database connection and context
func Init(db *sql.DB) {
	dbh = db
	ctx = context.Background()
}

// Exec execute all tasks in the scheduler
func (s *Scheduler) Exec() error {
	fmt.Println("Launching scheduler")
	for {
		//Get all tasks waiting in db
		fmt.Println("Checking new tasks")
		todoTasks, err := m.Tasks(qm.Where("todo_date<?", time.Now()), qm.And("status=?", m.TaskStatusTodo)).All(ctx, dbh)
		if err != nil {
			return err
		}

		for _, todoTaskDB := range todoTasks {
			//Find corresponding task
			todoTask := &UserTask{
				Task: todoTaskDB,
			}
			var fnt Task
			for _, findNewTask := range s.Tasks {
				if findNewTask.Name == todoTask.Name {
					fnt = findNewTask
				}
			}

			if fnt.Name == "" {
				//TODO:Log error and commit status error
				fmt.Println("tasker: task type not found")
				continue
			}

			execTask := fnt
			execTask.UserTask = todoTask

			err = execTask.Exec()
			if err != nil && err == ErrReachedMaxRetry {
				//TODO:Log error and commit status error
				fmt.Println("Task reached max retry count, setting state error")
				execTask.UserTask.Status = m.TaskStatusError
			} else if err != nil {
				pretty.Println(err)
			}

			fmt.Println("Update task in DB")
			err := execTask.UserTask.UpdateDB()
			if err != nil {
				return err
			}
			fmt.Println("Ok")
		}

		time.Sleep(time.Second)
	}
}

// Exec execute at task
func (t *Task) Exec() error {
	err := t.initValidate()
	if err != nil {
		return err
	}

	actStep, err := t.getActualStep()
	for {
		err = actStep.Exec(t)
		if err != nil {
			fmt.Printf("Step %s failed : %s\n", actStep.Name, err.Error())

			if t.UserTask.Retry+1 >= t.MaxRetry {
				return ErrReachedMaxRetry
			}

			t.UserTask.Retry++
			return nil
		}

		actStep, err = t.getNextStep()

		if err == ErrReachedEndOfTask {
			t.UserTask.Status = m.TaskStatusDone
			return nil
		} else if err != nil {
			return err
		}

		t.UserTask.ActualStep = actStep.Name

		err = t.UserTask.UpdateDB()
		if err != nil {
			return err
		}
	}

}

func (t *Task) initValidate() error {
	if len(t.Steps) == 0 {
		return ErrMissingSteps
	}

	if t.Name == "" {
		return ErrMissingTaskName
	}

	for _, s := range t.Steps {
		if s.Name == "" {
			return ErrMissingTaskName
		}
		if s.Exec == nil {
			return ErrMissingExecFunction
		}
	}

	if t.UserTask == nil {
		return ErrNilUserTask
	}

	if t.UserTask.ActualStep == "" {
		t.UserTask.ActualStep = t.Steps[0].Name
	}

	if t.UserTask.Status == "" {
		t.UserTask.Status = m.TaskStatusTodo
	}

	return nil
}

func (t *Task) getActualStep() (*Step, error) {
	if t.UserTask.ActualStep == "" {
		return &t.Steps[0], nil
	}

	// Find the next step
	for _, s := range t.Steps {
		if s.Name == t.UserTask.ActualStep {
			return &s, nil
		}
	}

	return nil, ErrStepNotFound
}

func (t *Task) getNextStep() (*Step, error) {
	if t.UserTask.ActualStep == "" {
		return nil, ErrStepNotFound
	}

	// Find the next step
	for i, s := range t.Steps {
		if s.Name == t.UserTask.ActualStep {
			if i+1 < len(t.Steps) {
				return &t.Steps[i+1], nil
			}

			// No next step, we finished the work
			return nil, ErrReachedEndOfTask
		}
	}

	return nil, ErrStepNotFound
}
