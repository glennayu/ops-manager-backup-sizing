package components
import (
	"testing"
	"fmt"
)

const wt_port = 28000
const wt_port_defPath = 28001

func TestGetWTFilesSize(test *testing.T) {
	session := dial(wt_port)
	defer session.Close()

	f, err := getWTFileSize(session)
	fmt.Println(f, err)

	sess_defPath := dial(wt_port_defPath)
	defer sess_defPath.Close()
	f, err = getWTFileSize(sess_defPath)
	fmt.Println(f, err)

}
