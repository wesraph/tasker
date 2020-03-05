package tasker

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/kr/pretty"
	"github.com/volatiletech/sqlboiler/boil"
	"github.com/wesraph/tasker/models"
	m "github.com/wesraph/tasker/models"
)

func init() {
	getDBHandler()
}

// Buffer is the task buffer
type Buffer struct {
	UserAddress string `json:"user_address"`
	NodeID      string `json:"node_id"`
}

func getBuffer(t *UserTask) (*Buffer, error) {
	if t.Buffer != nil {
		return t.Buffer.(*Buffer), nil
	}

	t.Buffer = &Buffer{}
	if !t.UserBuffer.Valid {
		return t.Buffer.(*Buffer), nil
	}
	return t.Buffer.(*Buffer), json.Unmarshal(t.UserBuffer.JSON, t.Buffer)
}

func testStep(t *Task) error {
	fmt.Println("step1")
	return nil
}

func testStepBuffer(t *Task) error {
	b, err := getBuffer(t.UserTask)
	if err != nil {
		return nil
	}

	b.UserAddress = "salutsalut"
	return nil
}

func testShowBuffer(t *Task) error {
	b, err := getBuffer(t.UserTask)
	if err != nil {
		return nil
	}

	pretty.Println(b)
	return nil
}

func testFailingStep(t *Task) error {
	return fmt.Errorf("test failing task")
}

func testStep2(t *Task) error {
	fmt.Println("step2")
	return nil
}

func testRenameNextStep(t *Task) error {
	t.UserTask.ActualStep = "doesnt_exists"
	return nil
}

func TestExec1(t *testing.T) {
	task := &Task{
		Name: "test",
		UserTask: &UserTask{
			Task: &m.Task{
				ID:         "c9f51923-293a-4e3b-a49f-cccd71db4679",
				ActualStep: "step1",
				CreatedAt:  time.Now(),
				TodoDate:   time.Now(),
				Status:     m.TaskStatusTodo,
				Retry:      0,
			},
		},
		Steps: []Step{
			{
				Name: "step1",
				Exec: testStep,
			},
			{
				Name: "step2",
				Exec: testStep2,
			},
		},
	}

	err := task.Exec()
	if err != nil {
		t.Errorf("Error while testing task execution")
	}
}

func TestExec2(t *testing.T) {
	task := &Task{
		Name: "test",
		UserTask: &UserTask{
			Task: &m.Task{
				ID:         "c9f51923-293a-4e3b-a49f-cccd71db4679",
				ActualStep: "step1",
				CreatedAt:  time.Now(),
				TodoDate:   time.Now(),
				Status:     m.TaskStatusTodo,
				Retry:      0,
			},
		},
		Steps: []Step{
			{
				Name: "step1",
				Exec: testStep,
			},
			{
				Name: "step2",
				Exec: testStep2,
			},
		},
	}

	err := task.Exec()
	if err != nil {
		t.Errorf("Error while testing task execution")
	}
}

func TestExec3(t *testing.T) {
	task := &Task{
		UserTask: &UserTask{
			Task: &models.Task{
				ID:         "c9f51923-293a-4e3b-a49f-cccd71db4679",
				ActualStep: "step1",
				CreatedAt:  time.Now(),
				TodoDate:   time.Now(),
				Status:     m.TaskStatusTodo,
				Retry:      0,
			},
		},
		Steps: []Step{
			{
				Name: "step1",
				Exec: testStep,
			},
			{
				Name: "step2",
				Exec: testStep2,
			},
		},
	}

	err := task.Exec()
	if err != ErrMissingTaskName {
		t.Errorf("Error while testing task execution")
	}
}

func TestRetry(t *testing.T) {
	task := &Task{
		Name: "test",
		UserTask: &UserTask{
			Task: &models.Task{
				ID:         "c9f51923-293a-4e3b-a49f-cccd71db4679",
				ActualStep: "step1",
				CreatedAt:  time.Now(),
				TodoDate:   time.Now(),
				Status:     m.TaskStatusTodo,
				Retry:      0,
			},
		},
		Steps: []Step{
			{
				Name: "step1",
				Exec: testStep,
			},
			{
				Name: "step2",
				Exec: testFailingStep,
			},
		},
	}

	err := task.Exec()
	if err == nil {
		t.Errorf("Should fail because of retry")
	}
}

func UnnamedTask(t *testing.T) {
	task := &Task{
		Name: "test",
		UserTask: &UserTask{
			Task: &models.Task{
				ID:         "c9f51923-293a-4e3b-a49f-cccd71db4679",
				ActualStep: "step1",
				CreatedAt:  time.Now(),
				TodoDate:   time.Now(),
				Status:     m.TaskStatusTodo,
				Retry:      0,
			},
		},
		Steps: []Step{
			{
				Name: "step1",
				Exec: testStep,
			},
			{
				Name: "step2",
				Exec: testFailingStep,
			},
		},
	}

	err := task.Exec()
	if err == nil {
		t.Errorf("Should fail because of retry")
	}
}

func RenameTask(t *testing.T) {
	task := &Task{
		Name: "test",
		UserTask: &UserTask{
			Task: &m.Task{
				ActualStep: "step1",
				CreatedAt:  time.Now(),
				TodoDate:   time.Now(),
				Status:     m.TaskStatusTodo,
				Retry:      0,
			}},
		Steps: []Step{
			{
				Name: "step1",
				Exec: testRenameNextStep,
			},
			{
				Name: "step2",
				Exec: testStep,
			},
		},
	}

	err := task.Exec()
	if err == nil {
		t.Errorf("Should fail because of retry")
	}
}

func getDBHandler() error {
	if dbh != nil {
		return nil
	}

	var err error
	dbh, err = sql.Open("postgres", "host=localhost port=5433 sslmode=disable dbname=test user=root password=root")
	if err != nil {
		return err
	}

	Init(dbh)

	return nil
}

func cleanDB(table string) error {
	err := getDBHandler()
	if err != nil {
		return err
	}

	_, err = dbh.Exec("DELETE FROM tasks")
	if err != nil {
		return err
	}

	return nil
}

func TestScheduler(t *testing.T) {
	err := getDBHandler()
	if err != nil {
		t.Errorf("Cannot get db handler")
	}

	err = cleanDB("tasks")
	if err != nil {
		t.Errorf("Cannot clean db:" + err.Error())
	}

	userTask := &m.Task{
		Name:       "test",
		ActualStep: "step1",
		CreatedAt:  time.Now(),
		TodoDate:   time.Now(),
		Status:     m.TaskStatusTodo,
		Retry:      0,
	}
	err = userTask.Insert(ctx, dbh, boil.Infer())
	if err != nil {
		t.Errorf("Cannot insert task in db")
	}

	userTask2 := &m.Task{
		Name:       "test",
		ActualStep: "step1",
		CreatedAt:  time.Now(),
		TodoDate:   time.Now(),
		Status:     m.TaskStatusTodo,
		Retry:      0,
	}
	err = userTask2.Insert(ctx, dbh, boil.Infer())
	if err != nil {
		t.Errorf("Cannot insert task in db")
	}
	s := &Scheduler{
		Tasks: []Task{
			Task{
				Name: "test",
				Steps: []Step{
					{
						Name: "step1",
						Exec: testStep,
					},
					{
						Name: "stepBuffer",
						Exec: testStepBuffer,
					},
					{
						Name: "step2",
						Exec: testStep2,
					},
					{
						Name: "stepShowBuffer",
						Exec: testShowBuffer,
					},
				},
			},
		},
	}

	err = s.Exec()
	if err != nil {
		t.Errorf("Failing using scheduler" + err.Error())
	}
}
