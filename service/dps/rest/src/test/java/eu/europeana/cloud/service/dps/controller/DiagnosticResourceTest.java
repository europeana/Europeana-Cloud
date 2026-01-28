package eu.europeana.cloud.service.dps.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskDiagnosticInfo;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class DiagnosticResourceTest {

  @InjectMocks
  DiagnosticResource resource;

  @Mock
  TaskDiagnosticInfoDAO taskDiagnosticInfoDAO;

  @Mock
  CassandraTaskInfoDAO taskInfoDAO;

  @Mock
  TasksByStateDAO tasksByStateDAO;

  @Test
  void shouldReturnDiagnosticForTaskWithUncomleteInformation() throws IOException {
    when(taskInfoDAO.findById(anyLong())).thenReturn(Optional.of(
            TaskInfo.builder().id(10).startTimestamp(new Date()).build()));
    when(taskDiagnosticInfoDAO.findById(anyLong())).thenReturn(Optional.empty());
    when(tasksByStateDAO.findTask(any(), any(), anyLong())).thenReturn(Optional.empty());

    String json = resource.task(10);

    System.out.println(json);
    assertNotNull(new ObjectMapper().readTree(json));
  }

  @Test
  void shouldReturnDiagnosticForTaskWithCompleteInformation() throws IOException {
    when(taskInfoDAO.findById(anyLong())).thenReturn(Optional.of(
            TaskInfo.builder().id(10).startTimestamp(new Date()).build()));
    when(taskDiagnosticInfoDAO.findById(anyLong())).thenReturn(Optional.of(TaskDiagnosticInfo.builder()
            .finishOnStormTime(Instant.now())
            .build()));
    TaskByTaskState s = new TaskByTaskState();
    s.setTopicName("topic");
    when(tasksByStateDAO.findTask(any(), any(), anyLong())).thenReturn(Optional.of(s));

    String json = resource.task(10);

    System.out.println(json);
    assertNotNull(new ObjectMapper().readTree(json));
  }
}