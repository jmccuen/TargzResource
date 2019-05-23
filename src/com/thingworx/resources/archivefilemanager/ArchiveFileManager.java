package com.thingworx.resources.archivefilemanager;

import com.thingworx.common.RESTAPIConstants;
import com.thingworx.common.exceptions.InvalidRequestException;
import com.thingworx.common.utils.PathUtilities;
import com.thingworx.data.util.InfoTableInstanceFactory;
import com.thingworx.entities.utils.ThingUtilities;
import com.thingworx.logging.LogUtilities;
import com.thingworx.metadata.annotations.ThingworxServiceDefinition;
import com.thingworx.metadata.annotations.ThingworxServiceParameter;
import com.thingworx.metadata.annotations.ThingworxServiceResult;
import com.thingworx.resources.Resource;
import com.thingworx.things.Thing;
import com.thingworx.things.repository.FileRepositoryThing;
import com.thingworx.things.repository.FileRepositoryThing.FileMode;
import com.thingworx.types.BaseTypes;
import com.thingworx.types.InfoTable;
import com.thingworx.types.collections.ValueCollection;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.UUID;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import org.slf4j.Logger;

@SuppressWarnings("serial")
public class ArchiveFileManager extends Resource {

	private static Logger _logger = LogUtilities.getInstance().getApplicationLogger(ArchiveFileManager.class);

	@ThingworxServiceDefinition(name = "GetCompressedFileInfo", description = "", category = "", isAllowOverride = false, aspects = {
			"isAsync:false" })
	@ThingworxServiceResult(name = "result", description = "", baseType = "INFOTABLE", aspects = {
			"isEntityDataShape:true", "dataShape:PTC.SCA.SCO.EXT.CompressedFileInfoDS" })
	public InfoTable GetCompressedFileInfo(
			@ThingworxServiceParameter(name = "fileRepository", description = "", baseType = "STRING") String fileRepository,
			@ThingworxServiceParameter(name = "path", description = "", baseType = "STRING") String path,
			@ThingworxServiceParameter(name = "compressionType", description = "", baseType = "STRING") String compressionType)
			throws Exception {
		_logger.trace("Entering Service: GetCompressedFileInfo");

		PathUtilities.validatePath(path);

		if (!(fileRepository != null && !fileRepository.isEmpty())) {
			throw new InvalidRequestException("File Repository Must Be Specified",
					RESTAPIConstants.StatusCode.STATUS_NOT_ACCEPTABLE);
		}

		Thing thing = ThingUtilities.findThing(fileRepository);
		if (thing == null) {
			throw new InvalidRequestException("File Repository [" + fileRepository + "] Does Not Exist",
					RESTAPIConstants.StatusCode.STATUS_NOT_FOUND);
		}
		if (!(thing instanceof FileRepositoryThing)) {
			throw new InvalidRequestException("Thing [" + fileRepository + "] Is Not A File Repository",
					RESTAPIConstants.StatusCode.STATUS_NOT_FOUND);
		}
		FileRepositoryThing repo = (FileRepositoryThing) thing;
		InfoTable it = InfoTableInstanceFactory.createInfoTableFromDataShape("PTC.SCA.SCO.EXT.CompressedFileinfoDS");
		FileInputStream fin;

		try {
			fin = repo.openFileForRead(path);
		} catch (Exception eOpen) {
			throw new InvalidRequestException(
					"Unable To Open [" + path + "] in [" + fileRepository + "] : " + eOpen.getMessage(),
					RESTAPIConstants.StatusCode.STATUS_NOT_FOUND);
		}

		try {
			BufferedInputStream in = new BufferedInputStream(fin);
			try {
				GzipCompressorInputStream gzIn = new GzipCompressorInputStream(in);
				try {
					TarArchiveInputStream tarIn = new TarArchiveInputStream(gzIn);
					try {
						TarArchiveEntry entry = null;
						while ((entry = (TarArchiveEntry) tarIn.getNextEntry()) != null) {
							if (entry.isFile()) {
								ValueCollection values = new ValueCollection();
								values.put("fileName", BaseTypes.ConvertToPrimitive(entry.getName(), BaseTypes.STRING));
								values.put("size", BaseTypes.ConvertToPrimitive(entry.getSize(), BaseTypes.LONG));
								it.addRow(values);

							}
						}
					} catch (Exception e) {

					} finally {
						tarIn.close();
					}
				} catch (Exception e) {

				} finally {
					gzIn.close();
				}
			} catch (Exception e) {

			}
		} catch (Exception e) {

		} finally {
			fin.close();
		}

		_logger.trace("Exiting Service: GetCompressedFileInfo");

		return it;
	}

	@ThingworxServiceDefinition(name = "RemoveEntryFromArchive", description = "", category = "", isAllowOverride = false, aspects = {
			"isAsync:false" })
	@ThingworxServiceResult(name = "result", description = "", baseType = "BOOLEAN", aspects = {})
	public Boolean RemoveEntryFromArchive(
			@ThingworxServiceParameter(name = "fileRepository", description = "", baseType = "STRING") String fileRepository,
			@ThingworxServiceParameter(name = "archivePath", description = "", baseType = "STRING") String archivePath,
			@ThingworxServiceParameter(name = "fileName", description = "", baseType = "STRING") String fileName,
			@ThingworxServiceParameter(name = "compressionType", description = "", baseType = "STRING") String compressionType)
			throws Exception {
		_logger.trace("Entering Service: RemoveEntryFromArchive");

		PathUtilities.validatePath(archivePath);

		if (!(fileRepository != null && !fileRepository.isEmpty())) {
			throw new InvalidRequestException("File Repository Must Be Specified",
					RESTAPIConstants.StatusCode.STATUS_NOT_ACCEPTABLE);
		}

		Thing thing = ThingUtilities.findThing(fileRepository);
		if (thing == null) {
			throw new InvalidRequestException("File Repository [" + fileRepository + "] Does Not Exist",
					RESTAPIConstants.StatusCode.STATUS_NOT_FOUND);
		}
		if (!(thing instanceof FileRepositoryThing)) {
			throw new InvalidRequestException("Thing [" + fileRepository + "] Is Not A File Repository",
					RESTAPIConstants.StatusCode.STATUS_NOT_FOUND);
		}

		FileRepositoryThing repo = (FileRepositoryThing) thing;
		FileInputStream fin;
		try {
			fin = repo.openFileForRead(archivePath);
		} catch (Exception eOpen) {
			throw new InvalidRequestException(
					"Unable To Open [" + archivePath + "] in [" + fileRepository + "] : " + eOpen.getMessage(),
					RESTAPIConstants.StatusCode.STATUS_NOT_FOUND);
		}

		Boolean complete = false;
		UUID uuid = UUID.randomUUID();
		
		String tempPath = archivePath + "-TempTarGz-" + uuid.toString() + ".tar.gz";

		repo.CreateTextFile(tempPath, "", true);

		FileOutputStream fout;
		try {
			fout = repo.openFileForWrite(tempPath, FileMode.WRITE);
		} catch (Exception e) {
			throw new InvalidRequestException(
					"Unable To Open [" + archivePath + "] in [" + fileRepository + "] : " + e.getMessage(),
					RESTAPIConstants.StatusCode.STATUS_NOT_FOUND);
		}

		try {
			BufferedInputStream in = new BufferedInputStream(fin);
			BufferedOutputStream out = new BufferedOutputStream(fout);
			try {
				GzipCompressorInputStream gzIn = new GzipCompressorInputStream(in);
				GzipCompressorOutputStream gzOut = new GzipCompressorOutputStream(out);
				try {
					TarArchiveInputStream tarIn = new TarArchiveInputStream(gzIn);
					TarArchiveOutputStream tarOut = new TarArchiveOutputStream(gzOut);

					tarOut.setBigNumberMode(2);
					tarOut.setLongFileMode(2);
					try {
						TarArchiveEntry entry = null;
						while ((entry = (TarArchiveEntry) tarIn.getNextEntry()) != null) {
							if (!entry.getName().equals(fileName) && entry.isFile()) {
								File entryFile = new File(entry.getFile(),entry.getName());
								TarArchiveEntry tarEntry = new TarArchiveEntry(entryFile);
								tarEntry.setSize(entry.getSize());

								tarOut.putArchiveEntry(tarEntry);

								int bytesRead = 0;
								byte[] buffer = new byte[256];
								while ((bytesRead = tarIn.read(buffer)) != -1) {
									tarOut.write(buffer, 0, bytesRead);
								}

								tarOut.closeArchiveEntry();

							}
						}
						complete = true;
					} catch (Exception e) {
						repo.DeleteFile(tempPath);
					} finally {
						tarIn.close();
						tarOut.close();

					}
				} catch (Exception e) {
					repo.DeleteFile(tempPath);
				} finally {
					gzIn.close();
					gzOut.close();
				}
			} catch (Exception e) {

			} finally {
				in.close();
				out.close();
			}
		} catch (Exception e) {

		} finally {
			fin.close();
			fout.close();
		}

		if (complete) {
			try {
				repo.MoveFile(tempPath, archivePath, true);
			} catch (Exception e) {
				repo.DeleteFile(tempPath);
				_logger.debug(e.getMessage());
			}
		}

		_logger.trace("Exiting Service: RemoveEntryFromArchive");
		return complete;
	}

}
