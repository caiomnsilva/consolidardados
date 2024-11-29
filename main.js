function consolidarDados() {
    const config = getConfig();
    if (!config.folderId) {
      throw new Error("ID da pasta não configurado. Use o Web App para configurar.");
    }
  
    const folder = DriveApp.getFolderById(config.folderId);
    const planilhaDestino = SpreadsheetApp.getActiveSpreadsheet();
    const configSheets = getSheetConfig();
    const abaConsolidacao = planilhaDestino.getSheetByName(configSheets.abaConsolidacao) || planilhaDestino.insertSheet(configSheets.abaConsolidacao);
    const abaLog = planilhaDestino.getSheetByName(configSheets.abaLog) || planilhaDestino.insertSheet(configSheets.abaLog);
  
    configurarCabecalhos(abaConsolidacao, configSheets.colunasConsolidacao);
    configurarCabecalhos(abaLog, configSheets.colunasLog);
  
    const ultimaDataProcessada = obterUltimaDataProcessamento(abaLog);
    const arquivos = buscarArquivosRecursivamente(folder, ultimaDataProcessada);
  
    console.log(`Total de arquivos encontrados: ${arquivos.length}`);
  
    const dataExecucao = new Date();
    let totalProcessados = 0;
    let totalIgnorados = 0;
  
    const bufferDados = [];
    const bufferLogs = [];
  
    // Mapeamento de INEPs para seus dados e arquivos mais recentes
    const inepMap = new Map();
    const inepsConsolidados = obterInepsConsolidados(abaConsolidacao);
  
    arquivos.forEach((file, index) => {
      console.time(`Processamento do arquivo ${index + 1}`);
      try {
        console.log(`Processando arquivo ${index + 1} de ${arquivos.length}: ${file.getName()}`);
        const resultado = processarArquivo(file, dataExecucao);
  
        if (resultado.sucesso) {
          const inepAtual = resultado.dados[0][1]; // INEP é a 2ª coluna do dado
          const dataArquivo = file.getDateCreated();
  
          const arquivoExistente = inepMap.get(inepAtual);
          if (arquivoExistente) {
            const dataExistente = arquivoExistente.file.getDateCreated();
            if (dataArquivo > dataExistente) {
              // Remove dados antigos e atualiza com novos dados
              removerDadosConsolidados(abaConsolidacao, inepAtual, resultado.dados);
              // Atualiza o mapa com o novo arquivo
              inepMap.set(inepAtual, { file, dados: resultado.dados });
              bufferDados.push(...resultado.dados);
              bufferLogs.push([dataExecucao, file.getName(), file.getUrl(), inepAtual, 'Atualizado', resultado.dados.length, 'Dados atualizados com novo arquivo', dataArquivo]);
              totalProcessados++;
            } else {
              bufferLogs.push([dataExecucao, file.getName(), file.getUrl(), inepAtual, 'Ignorado', 0, 'Arquivo mais antigo já consolidado', dataArquivo]);
              totalIgnorados++;
            }
          } else {
            // Adiciona o INEP ao mapa
            inepMap.set(inepAtual, { file, dados: resultado.dados });
            bufferDados.push(...resultado.dados);
            bufferLogs.push([dataExecucao, file.getName(), file.getUrl(), inepAtual, 'Processado', resultado.dados.length, 'Sucesso', dataArquivo]);
            totalProcessados++;
          }
        } else {
          bufferLogs.push([dataExecucao, file.getName(), file.getUrl(), '', 'Ignorado', 0, resultado.motivo, file.getDateCreated()]);
          totalIgnorados++;
        }
      } catch (error) {
        bufferLogs.push([dataExecucao, file.getName(), file.getUrl(), '', 'Erro', 0, error.message, file.getDateCreated()]);
      }
      console.timeEnd(`Processamento do arquivo ${index + 1}`);
    });
  
    // Grava dados e logs em uma única operação
    gravarDados(abaConsolidacao, bufferDados);
    gravarLogs(abaLog, bufferLogs);
  
    console.log(`Consolidação finalizada. Processados: ${totalProcessados}, Ignorados: ${totalIgnorados}`);
    enviarResumoPorEmail(totalProcessados, totalIgnorados, dataExecucao, arquivos.length, bufferLogs);
  
    // Verificação final do total de linhas consolidadas
    const totalLinhasConsolidadas = abaConsolidacao.getLastRow() - 1; // Desconta o cabeçalho
    console.log(`Total de linhas consolidadas: ${totalLinhasConsolidadas}`);
  }
  
  function processarArquivo(file, dataExecucao) {
    const fileId = file.getId();
    const fileUrl = file.getUrl();
    const filePath = obterCaminhoArquivo(DriveApp.getFolderById(getConfig().folderId), file);
  
    const planilhaOrigem = SpreadsheetApp.open(file);
    const abaOrigem = planilhaOrigem.getSheets()[0];
  
    const escola = abaOrigem.getRange("I8").getValue();
    const inep = String(abaOrigem.getRange("I9").getValue()).replace(/\D/g, "").trim(); // Limpa INEP
    const regional = abaOrigem.getRange("I10").getValue();
  
    const linhasOrigem = abaOrigem.getLastRow() - 15;
    if (linhasOrigem < 46) {
      return { sucesso: false, fileUrl, filePath, motivo: "Menos de 46 linhas de dados no arquivo" };
    }
  
    const numeroSerie = abaOrigem.getRange(16, 9, Math.min(linhasOrigem, 46)).getValues();
    const tagJ = abaOrigem.getRange(16, 10, Math.min(linhasOrigem, 46)).getValues();
    const tagK = abaOrigem.getRange(16, 11, Math.min(linhasOrigem, 46)).getValues();
    const tombamento = abaOrigem.getRange(16, 12, Math.min(linhasOrigem, 46), 4).getValues();
  
    const dados = [];
    for (let i = 0; i < Math.min(linhasOrigem, 46); i++) {
      const numeroSerieAtual = numeroSerie[i][0];
      const tagAtual = (tagJ[i][0] || "") + (tagK[i][0] || "");
      const tombamentoAtual = tombamento[i].filter(val => val).join(" ");
  
      const { tagValida, excedente } = processarTag(tagAtual);
      const tombamentoFinal = `${excedente} ${tombamentoAtual}`.trim();
  
      if (numeroSerieAtual || tagValida || tombamentoFinal) {
        dados.push([regional, inep, escola, numeroSerieAtual, tagValida, tombamentoFinal, dataExecucao, fileId]);
      }
    }
  
    return { sucesso: true, fileUrl, filePath, dados };
  }
  
  function removerDadosConsolidados(abaConsolidacao, inep, novosDados) {
    const rangeDados = abaConsolidacao.getDataRange();
    const dados = rangeDados.getValues();
    const novasIds = new Set(novosDados.map(d => d[7])); // IDs dos novos dados
    for (let i = dados.length - 1; i >= 1; i--) {
      if (dados[i][1] === inep && !novasIds.has(dados[i][7])) {
        abaConsolidacao.deleteRow(i + 1);
      }
    }
  }
  
  function obterCaminhoArquivo(pastaRaiz, arquivo) {
    let caminho = arquivo.getName();
    let pastaAtual = arquivo.getParents().next();
    while (pastaAtual && pastaAtual.getId() !== pastaRaiz.getId()) {
      caminho = `${pastaAtual.getName()} > ${caminho}`;
      pastaAtual = pastaAtual.getParents().hasNext() ? pastaAtual.getParents().next() : null;
    }
    return caminho;
  }
  
  function obterInepsConsolidados(abaConsolidacao) {
    const ultimaLinha = abaConsolidacao.getLastRow();
    if (ultimaLinha <= 1) {
      return [];
    }
    return abaConsolidacao.getRange(2, 2, ultimaLinha - 1, 1).getValues().flat();
  }
  
  function configurarCabecalhos(aba, colunas) {
    if (aba.getLastRow() === 0) {
      aba.appendRow(colunas);
    }
  }
  
  function gravarDados(abaConsolidacao, dados) {
    if (dados && dados.length > 0) {
      const ultimaLinha = abaConsolidacao.getLastRow();
      abaConsolidacao.getRange(ultimaLinha + 1, 1, dados.length, dados[0].length).setValues(dados);
    } else {
      console.log("Nenhum dado para consolidar.");
    }
  }
  
  function gravarLogs(abaLog, logs) {
    if (logs && logs.length > 0) {
      const ultimaLinha = abaLog.getLastRow();
      abaLog.getRange(ultimaLinha + 1, 1, logs.length, logs[0].length).setValues(logs);
    } else {
      console.log("Nenhum log para gravar.");
    }
  }
  
  function enviarResumoPorEmail(totalProcessados, totalIgnorados, dataExecucao, totalArquivos, logs) {
    const destinatario = Session.getEffectiveUser().getEmail();
    const assunto = "Resumo da Consolidação de Dados";
  
    // Mapeamento de INEPs processados e ignorados
    const inepsProcessados = logs
      .filter(log => log[4] === 'Processado' || log[4] === 'Atualizado')
      .map(log => log[3]) // Coluna INEP
      .join(", ");
  
    const inepsIgnorados = logs
      .filter(log => log[4] === 'INEP duplicado' || log[4] === 'Ignorado')
      .map(log => log[3]) // Coluna INEP
      .join(", ");
  
    // Corpo do e-mail aprimorado
    const corpo = `
  Resumo da execução:
  - Data e Hora da execução: ${dataExecucao.toLocaleString()}
  - Total de arquivos encontrados: ${totalArquivos}
  - Total de registros processados: ${totalProcessados}
  - Total de registros ignorados: ${totalIgnorados}
  
  Detalhes:
  - INEPs processados (${totalProcessados}): ${inepsProcessados || "Nenhum INEP processado."}
  - INEPs ignorados (${totalIgnorados}): ${inepsIgnorados || "Nenhum INEP ignorado."}
  
  Observações:
  - INEPs atualizados substituíram registros anteriores.
  - Arquivos ignorados podem ter sido duplicados ou conter dados inválidos.
  
  Logs detalhados estão disponíveis na aba "Log" da planilha de consolidação.
    `;
  
    GmailApp.sendEmail(destinatario, assunto, corpo);
  }
  
  function buscarArquivosRecursivamente(pasta, ultimaDataProcessada = null) {
    let arquivos = [];
    const arquivosPastaAtual = pasta.getFilesByType(MimeType.GOOGLE_SHEETS);
  
    while (arquivosPastaAtual.hasNext()) {
      const arquivo = arquivosPastaAtual.next();
      if (!ultimaDataProcessada || arquivo.getDateCreated() > ultimaDataProcessada) {
        arquivos.push(arquivo);
      }
    }
  
    const subpastas = pasta.getFolders();
    while (subpastas.hasNext()) {
      arquivos = arquivos.concat(buscarArquivosRecursivamente(subpastas.next(), ultimaDataProcessada));
    }
  
    // Ordena arquivos por data de criação descendente
    arquivos.sort((a, b) => b.getDateCreated() - a.getDateCreated());
  
    return arquivos;
  }
  
  function obterUltimaDataProcessamento(abaLog) {
    const ultimaLinha = abaLog.getLastRow();
    if (ultimaLinha <= 1) {
      return null;
    }
    const ultimaData = abaLog.getRange(ultimaLinha, 1).getValue();
    return new Date(ultimaData);
  }
  
  function processarTag(tag) {
    const caracteresValidos = String(tag || "").replace(/\s+/g, "");
    const tagValida = caracteresValidos.slice(0, 6);
    const excedente = caracteresValidos.slice(6);
    return { tagValida, excedente };
  }
  
  function getSheetConfig() {
    return {
      abaConsolidacao: "Consolidacao",
      abaLog: "Log",
      colunasConsolidacao: ["Regional", "INEP", "Escola", "Número de Série", "Tag", "Tombamento", "Data de Consolidação", "ID do Arquivo"],
      colunasLog: ["Data e Hora", "Arquivo", "Link do Arquivo", "INEP", "Status", "Linhas Consolidadas", "Resumo", "Data do Arquivo"]
    };
  }