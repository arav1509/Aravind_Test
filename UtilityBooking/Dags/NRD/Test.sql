BEGIN
CALL {{ params.proc_project }}.{{ params.proc_dataset }}.{{ params.procedure_name }}();
EXCEPTION WHEN ERROR THEN
RAISE;
END;