#for i in {0..10}; do go test -run TestFigure82CMulti -race; done
#for i in {0..10}; do go test -run 2A -race >> out; done
#for i in {0..10}; do go test -run 2B -race >> out; done
for i in {0..100}; do go test -run TestBackupSelfDefined -race >> out; done
