package tasker

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/kr/pretty"
	_ "github.com/lib/pq"
	"github.com/volatiletech/sqlboiler/queries/qm"

	"github.com/volatiletech/sqlboiler/boil"
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
	UserTask *m.Task
	MaxRetry int
}

// Scheduler is a group of tasks
type Scheduler struct {
	Tasks []Task
}

func init() {
}

// Init the database connection and context
func Init(db *sql.DB) {
	boil.SetDB(dbh)
	ctx = context.Background()
}

// Exec execute all tasks in the scheduler
func (s *Scheduler) Exec() error {
	fmt.Println("Launching scheduler")
	for {
		//Get all tasks waiting in db
		todoTasks, err := m.Tasks(qm.Where("todo_date<?", time.Now()), qm.And("status=?", m.TaskStatusTodo)).All(ctx, dbh)
		if err != nil {
			return err
		}

		for _, todoTask := range todoTasks {
			//Find corresponding task
			var fnt Task
			for _, findNewTask := range s.Tasks {
				if findNewTask.Name == todoTask.Name {
					fnt = findNewTask
				}
			}

			if fnt.Name == "" {
				//TODO:Log error and commit status error
				continue
			}

			execTask := fnt
			execTask.UserTask = todoTask

			fmt.Println("Executing task")
			pretty.Println(execTask)

			err = execTask.Exec()
			if err != nil {
				//TODO:Log error and commit status error
				pretty.Println(err)
			}
		}
		break
	}
	return nil
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
			if t.UserTask.Retry+1 >= t.MaxRetry {
				return ErrReachedMaxRetry
			}

			t.UserTask.Retry++
		} else {
			actStep, err = t.getNextStep()

			if err == ErrReachedEndOfTask {
				t.UserTask.Status = m.TaskStatusDone
				return nil
			} else if err != nil {
				return err
			}

			t.UserTask.ActualStep = actStep.Name
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
